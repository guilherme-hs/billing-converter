package com.tenacity.billing;

/**
 * Represents the Numbers to be matched from the file and possible replacements
 */
public class NumberMatcher {

    /**
     * Name of the Pattern
     */
    String name;

    /**
     * Pattern to be matched
     */
    String pattern;

    /**
     * Replacement String
     */
    String replacement;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getReplacement() {
        return replacement;
    }

    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }
}
