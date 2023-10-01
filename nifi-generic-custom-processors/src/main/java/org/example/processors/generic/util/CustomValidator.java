package org.example.processors.generic.util;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.jetbrains.annotations.NotNull;

import static org.example.processors.generic.util.Constant.VALIDATION_RESULT_DESCRIPTION;

public class CustomValidator {

    /**
     * Custom validation for property
     * @param subject
     * @param input
     * @param validationContext
     * @return
     */
    public static @NotNull ValidationResult customPropertyValidator(String subject, @NotNull String input, ValidationContext validationContext) {
        String[] parts = input.split(",");
        if (parts.length == 0) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation(VALIDATION_RESULT_DESCRIPTION)
                    .build();
        }
        return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(true)
                .build();
    }
}
