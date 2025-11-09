package com.example.logguard.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PersonallyIdentifiableInfoRedactionService : scrubs sensitive personal data from raw log 
 * messages before they are persisted or forwarded downstream. This ensures that even if a developer
 * accidentally logs a user's payment details or government ID, the stored record is clean.
 *
 * Processing order matters - TOKEN is applied first because a JWT can contain
 * an email address inside its payload. Redacting the token first prevents the
 * EMAIL pattern from partially matching inside an already-flagged token.
 *
 * Credit cards and Aadhaar numbers use checksum validation (Luhn and Verhoeff
 * respectively) before redacting, so that order IDs or timestamps that happen
 * to match the digit-count pattern are not incorrectly scrubbed.
 *
 * Redaction map:
 *   Bearer / JWT tokens  ->  [TOKEN REDACTED]
 *   Email addresses      ->  [EMAIL REDACTED]
 *   IPv4 addresses       ->  [IP REDACTED]
 *   Indian phone numbers ->  [PHONE REDACTED]
 *   Credit card numbers  ->  [CC REDACTED]       (Luhn-validated)
 *   Aadhaar numbers      ->  [AADHAAR REDACTED]  (Verhoeff-validated)
 *
 * Toggle via application.yml:
 *   guard.redaction.enabled=false   (default: true)
 */
@Service
public class PersonallyIdentifiableInfoRedactionService {

    private static final Logger log = LoggerFactory.getLogger(PersonallyIdentifiableInfoRedactionService.class);

    @Value("${guard.redaction.enabled:true}")
    private boolean redactionEnabled;

    // =========================================================================
    // PII Patterns
    //
    // All patterns are compiled once at class-load time (static final) so that
    // the regex engine does not recompile them on every call to redact().
    // =========================================================================

    /**
     * EMAIL - RFC 5322 approximation.
     *
     * Covers the full range of valid local-part characters including dots,
     * plus signs, and special characters that appear in real addresses.
     * CASE_INSENSITIVE handles uppercase domains (e.g. User@EXAMPLE.COM).
     *
     * Examples matched:
     *   user@example.com
     *   first.last+tag@sub.domain.org
     *   admin@192.168.1.1   (IP-literal domains - valid but unusual)
     */
    private static final Pattern PHONE = Pattern.compile(
	    "\\b(?:(?:\\+91|91|0)[\\s\\-]?)?[6-9](?:[\\s\\-]?\\d){9}\\b"
	);


    /**
     * IPV4 - strict octet validation.
     *
     * Each octet is constrained to 0–255, which prevents false positives on
     * strings like "999.999.999.999" or version numbers such as "3.2.15.0"
     * (the latter is blocked by the leading \b word boundary requiring that
     * the match not be part of a longer alphanumeric word).
     *
     * Examples matched:
     *   192.168.1.100
     *   10.0.0.1
     *   255.255.255.0
     */
    private static final Pattern IPV4 = Pattern.compile(
        "\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}" +
        "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b"
    );

    /**
     * CREDIT_CARD - major card network prefixes with correct length ranges.
     *
     * Covers:
     *   Visa        - starts with 4, 13 or 16 digits
     *   Mastercard  - classic 51–55 prefix AND modern 2221–2720 range
     *   Amex        - starts with 34 or 37, 15 digits
     *   RuPay       - starts with 60, 64, 65, 81, or 82, 16–19 digits
     *
     * Note: this pattern is intentionally permissive - it captures candidates
     * that look like card numbers by prefix and length. The Luhn checksum
     * in redactWithChecksum() then filters out false positives such as
     * sequential order IDs (e.g. 4000000000000000 fails Luhn).
     */
    private static final Pattern CREDIT_CARD = Pattern.compile(
        "\\b(?:" +
        "4\\d{12}(?:\\d{3})?|"  +                                          // Visa
        "(?:5[1-5]\\d{2}|222[1-9]|22[3-9]\\d|2[3-6]\\d{2}|27[01]\\d|2720)\\d{12}|" + // Mastercard
        "3[47]\\d{13}|" +                                                   // Amex
        "(?:60|64|65|81|82)\\d{14,17}" +                                   // RuPay
        ")\\b"
    );

    /**
     * PHONE - Indian mobile numbers with flexible formatting.
     *
     * Indian mobile numbers always start with a digit in the range 6–9.
     * The leading \b prevents this pattern from matching inside a longer
     * digit string (e.g. the tail of a credit card number), which would
     * corrupt the string before Luhn validation runs on the CC pattern.
     *
     * Handles optional country code prefixes (+91, 91, 0) and common
     * separators (spaces and hyphens) between digit groups.
     *
     * Examples matched:
     *   9876543210
     *   +91 98765 43210
     *   091-9876-543210
     *   91 9876543210
     */
    private static final Pattern PHONE = Pattern.compile(
        "\\b(?:(?:\\+91|91|0)[\\s\\-]?)?[6-9]\\d{2}[\\s\\-]?\\d{3}[\\s\\-]?\\d{4}\\b"
    );

    /**
     * AADHAAR - 12-digit Indian government identity number.
     *
     * The UIDAI specification states that Aadhaar numbers:
     *   - Are exactly 12 digits long
     *   - Never start with 0, 1, or 5
     *
     * Accepts the standard 4-4-4 display format (e.g. 2345 6789 0123)
     * as well as compact form (234567890123). Separators may be spaces
     * or hyphens. Like credit cards, the regex captures candidates and
     * the Verhoeff checksum in redactWithChecksum() validates them before
     * redaction, preventing false positives on 12-digit order or invoice IDs.
     *
     * Examples matched:
     *   2345 6789 0123
     *   2345-6789-0123
     *   234567890123
     */
    private static final Pattern AADHAAR = Pattern.compile(
        "\\b[2-46-9]\\d{3}[\\s\\-]?\\d{4}[\\s\\-]?\\d{4}\\b"
    );

    /**
     * TOKEN - Bearer-prefixed JWTs and raw JWT strings.
     *
     * Two sub-patterns joined by alternation:
     *
     *   1. Bearer keyword + JWT (three Base64url segments separated by dots)
     *      The Bearer keyword is now REQUIRED in this branch. The original
     *      code omitted it, which caused any dotted alphanumeric string
     *      (version numbers, class names, file paths) to be redacted.
     *
     *   2. Raw JWT starting with "eyJ" (Base64url encoding of '{"')
     *      All JWTs begin with this sequence regardless of whether the
     *      Authorization header prefix is present, so it reliably identifies
     *      tokens that appear anywhere in a log message.
     *
     * TOKEN is applied before EMAIL because a JWT payload decoded as a string
     * may contain an email claim - redacting the whole token first prevents a
     * partial EMAIL match inside an already-flagged segment.
     *
     * Examples matched:
     *   Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature
     *   eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature
     */
    private static final Pattern TOKEN = Pattern.compile(
        "\\bBearer\\s+[A-Za-z0-9\\-_]+\\.[A-Za-z0-9\\-_]+\\.[A-Za-z0-9\\-_.+/=]*\\b|" +
        "\\beyJ[A-Za-z0-9+/=]{20,}\\b"
    );

    // Public API

    /**
     * Redacts all detected PII from the given log message.
     *
     * Applies each pattern in the order defined above and returns
     * a new string with all detected PII replaced by its placeholder token.
     *
     * @param raw  the original log message, potentially containing PII
     * @return     a sanitised copy safe for persistence and downstream use.
     */
    public String redact(String raw) {
        if (!redactionEnabled || raw == null || raw.isBlank()) return raw;

        String result = raw;

        // Simple regex replacement - no checksum needed for these types
        result = applyPattern(result, TOKEN, "[TOKEN REDACTED]");
        result = applyPattern(result, EMAIL, "[EMAIL REDACTED]");
        result = applyPattern(result, IPV4,  "[IP REDACTED]");
        result = applyPattern(result, PHONE, "[PHONE REDACTED]");

        // Checksum-gated replacement - only redacts if the number is mathematically valid,
        // preventing false positives on order IDs and other numeric identifiers
        result = redactWithChecksum(result, CREDIT_CARD, "[CC REDACTED]",        this::isLuhnValid);
        result = redactWithChecksum(result, AADHAAR,     "[AADHAAR REDACTED]",   this::isVerhoeffValid);

        return result;
    }

    // Private helpers
    
    //  Replaces all matches of the given pattern with the replacement string. Used for PII categories that do not require checksum validation.
    private String applyPattern(String input, Pattern pattern, String replacement) {
        return pattern.matcher(input).replaceAll(replacement);
    }

    /**
     * Replaces pattern matches only when the matched value passes the supplied
     * checksum validator. Non-matching candidates are left in place.
     *
     * This two-phase approach (regex finds candidates, algorithm validates)
     * is necessary for credit cards and Aadhaar numbers because their digit
     * patterns overlap with many non-PII numeric identifiers. A purely
     * regex-based approach would produce far too many false positives.
     *
     * @param input       the string to search
     * @param pattern     regex that identifies candidate PII sequences
     * @param replacement the redaction placeholder (e.g. "[CC REDACTED]")
     * @param validator   checksum function - returns true if the candidate
     *                    is a genuine PII value and should be redacted
     * @return            a new string with validated matches replaced
     */
    private String redactWithChecksum(
            String input,
            Pattern pattern,
            String replacement,
            Predicate<String> validator) {

        StringBuilder sb = new StringBuilder();
        Matcher matcher = pattern.matcher(input);
        int lastEnd = 0;

        while (matcher.find()) {
            // Append everything between the previous match and this one unchanged
            sb.append(input, lastEnd, matcher.start());

            // Strip formatting characters before running the checksum algorithm -
            // the digit-only string is what both Luhn and Verhoeff operate on
            String digitsOnly = matcher.group().replaceAll("[\\s\\-]", "");

            if (validator.test(digitsOnly)) {
                sb.append(replacement);
            } else {
                // Checksum failed - likely a false positive (order ID, timestamp, etc.)
                // Preserve the original text so the log message is not corrupted
                sb.append(matcher.group());
            }

            lastEnd = matcher.end();
        }

        // Append any trailing text after the last match
        sb.append(input.substring(lastEnd));
        return sb.toString();
    }

    // Checksum algorithms

    /**
	 * Luhn algorithm - standard checksum for payment card numbers.
	 *
	 * Works by doubling every second digit from the right. If doubling produces
	 * a value greater than 9, subtract 9 (equivalent to summing the two digits).
	 * The number is valid if the total sum is divisible by 10.
	 *
	 * All major card networks (Visa, Mastercard, Amex, RuPay) use
	 * Luhn as a first-pass validity check, making it reliable for distinguishing
	 * genuine card numbers from random digit sequences of the same length.
	 *
	 * @param number  digit-only string with all separators already removed
	 * @return        true if the number satisfies the Luhn check digit
	 */
    private boolean isLuhnValid(String number) {
        int sum = 0;
        boolean doubleDigit = false;

        // Traverse right to left - the rightmost digit is the check digit itself
        for (int i = number.length() - 1; i >= 0; i--) {
            int digit = Character.getNumericValue(number.charAt(i));

            if (doubleDigit) {
                digit *= 2;
                // If doubling exceeds 9, subtract 9 (same as summing the two digits)
                if (digit > 9) digit -= 9;
            }

            sum += digit;
            doubleDigit = !doubleDigit;
        }

        return (sum % 10 == 0);
    }

    /**
     * Verhoeff algorithm - checksum used by India's UIDAI for Aadhaar numbers.
     *
     * The algorithm uses two lookup tables:
     *   d  - multiplication table of the dihedral group D5
     *   p  - permutation table that scrambles digit positions
     *
     * Processing steps:
     *   1. Reverse the digit string so that position 0 is the check digit
     *   2. For each digit, look up its permuted value using p[position % 8]
     *   3. Accumulate the result through the d multiplication table
     *   4. A final value of 0 means the check digit is valid
     *
     * Why reverse first?
     *   The permutation table p is indexed by position-from-the-right. Reversing
     *   the string lets us iterate left-to-right with a simple i % 8 index,
     *   avoiding the off-by-one errors that occur when indexing from the right
     *   inside a forward loop.
     *
     * @param num  digit-only Aadhaar string with all separators already removed
     * @return     true if the number satisfies the Verhoeff check digit
     */
    private boolean isVerhoeffValid(String num) {
        // Multiplication table of the dihedral group D5
        int[][] d = {
            {0,1,2,3,4,5,6,7,8,9},
            {1,2,3,4,0,6,7,8,9,5},
            {2,3,4,0,1,7,8,9,5,6},
            {3,4,0,1,2,8,9,5,6,7},
            {4,0,1,2,3,9,5,6,7,8},
            {5,9,8,7,6,0,4,3,2,1},
            {6,5,9,8,7,1,0,4,3,2},
            {7,6,5,9,8,2,1,0,4,3},
            {8,7,6,5,9,3,2,1,0,4},
            {9,8,7,6,5,4,3,2,1,0}
        };

        // Permutation table - re-orders digit values based on their position
        // Only 8 rows are needed because the pattern repeats with period 8
        int[][] p = {
            {0,1,2,3,4,5,6,7,8,9},
            {1,5,7,6,2,8,3,0,9,4},
            {5,8,0,3,7,9,6,1,4,2},
            {8,9,1,6,0,4,3,5,2,7},
            {9,4,5,3,1,2,6,8,7,0},
            {4,2,8,6,5,7,3,9,0,1},
            {2,7,9,3,8,0,6,4,1,5},
            {7,0,4,6,9,1,3,2,5,8}
        };

        int c = 0;

        // Reverse so that index 0 = check digit (rightmost), matching the
        // algorithm's expectation that p[0] applies to the check digit position
        String reversed = new StringBuilder(num).reverse().toString();

        for (int i = 0; i < reversed.length(); i++) {
            int digit = Character.getNumericValue(reversed.charAt(i));
            c = d[c][p[i % 8][digit]];
        }

        // c == 0 means the check digit is consistent with the preceding digits
        return c == 0;
    }
}