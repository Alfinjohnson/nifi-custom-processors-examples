package org.example.processors.generic.util;

public class Constant {



    private Constant() {
        throw new IllegalStateException("Utility class");
    }
    public static final String REL_FAILED_NAME = "Failed";
    public static final String REL_SUCCESS_NAME = "Success";
    public static final String KEY_NAMES = "KEYS";
    public static final String KEY_NAMES_DESCRIPTION = "coma separated key names";
    public static final String KEY_NAMES_PROPERTY_EXAMPLE = "AD,AF,BG";

    public static final String KEY_VALUES = "VALUES";
    public static final String KEY_VALUES_DESCRIPTION = "coma separated key values";
    public static final String KEY_VALUES_PROPERTY_EXAMPLE = "H1,H2,H3";

    public static final String KEY_TO_CHECK_NAME = "KEY_TO_CHECK";
    public static final String KEY_TO_CHECK_NAME_DESCRIPTION = "coma separated Json field names to check for the KEY";
    public static final String KEY_TO_CHECK_PROPERTY_EXAMPLE = "key1,key3";

    public static final String NEW_KEY_NAMES_DISPLAY = "NEW_KEY_NAMES";
    public static final String NEW_KEY_NAMES_DESCRIPTION = "coma separated new Json field names to be created";
    public static final String NEW_KEY_NAMES_PROPERTY_EXAMPLE = "cc_newField,cc_newField2";
    public static final String VALIDATION_RESULT_DESCRIPTION = "Input must be in the format: AA,AF,FG";

    public static final String RANGE_REGEX_NAME = "REGEX_NAME";
    public static final String RANGE_REGEX_NAMES_DESCRIPTION = "Regex Pattern";
    public static final String RANGE_REGEX_NAMES_PROPERTY_EXAMPLE = "@(.*)$";
    public static final String RANGE_FIELD_NAME = "FIELD_NAME";
    public static final String RANGE_FIELD_DESCRIPTION = "Json Field Name";
    public static final String RANGE_FIELD_PROPERTY_EXAMPLE = "cc_merch_name";
    public static final String JSONRegexMatcher_REGEX_NAME = "REGEX_NAME";
    public static final String JSONRegexMatcher_REGEX_NAMES_DESCRIPTION = "Regex Pattern";
    public static final String JSONRegexMatcher_REGEX_NAMES_PROPERTY_EXAMPLE = "@(.*)$";
    public static final String JSONRegexMatcher_FIELD_NAME = "FIELD_NAME";
    public static final String JSONRegexMatcher_FIELD_DESCRIPTION = "Json Field Name";
    public static final String JSONRegexMatcher_FIELD_PROPERTY_EXAMPLE = "cc_merch_name";

}
