package org.semagrow.sevod.scraper.rdf.dump.util;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

/**
 * Created by antonis on 25/5/2015.
 */
public class MyStringUtils {

    public static String forRegex(String aRegexFragment) {
        final StringBuilder result = new StringBuilder();

        final StringCharacterIterator iterator = new StringCharacterIterator(aRegexFragment);
        char character =  iterator.current();
        while (character != CharacterIterator.DONE ) {
            /*
            All literals need to have backslashes doubled.
            */
            if (character == '.') {
                result.append("\\.");
            }
            else if (character == '\\') {
                result.append("\\\\");
            }
            else if (character == '?') {
                result.append("\\?");
            }
            else if (character == '*') {
                result.append("\\*");
            }
            else if (character == '+') {
                result.append("\\+");
            }
            else if (character == '&') {
                result.append("\\&");
            }
            else if (character == ':') {
                result.append("\\:");
            }
            else if (character == '{') {
                result.append("\\{");
            }
            else if (character == '}') {
                result.append("\\}");
            }
            else if (character == '[') {
                result.append("\\[");
            }
            else if (character == ']') {
                result.append("\\]");
            }
            else if (character == '(') {
                result.append("\\(");
            }
            else if (character == ')') {
                result.append("\\)");
            }
            else if (character == '^') {
                result.append("\\^");
            }
            else if (character == '$') {
                result.append("\\$");
            }
            else {
                //the char is not a special one
                //add it to the result as is
                result.append(character);
            }
            character = iterator.next();
        }
        return result.toString();
    }
}
