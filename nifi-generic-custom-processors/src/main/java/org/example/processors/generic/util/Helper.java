package org.example.processors.generic.util;

import com.google.gson.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Helper {

    /**
     * read flow file and return contents in String
     * @return string
     */
    public static @NotNull String flowFileReader(FlowFile inputFlowFile, @NotNull ProcessSession session) {
        final StringBuilder content = new StringBuilder();
        session.read(inputFlowFile, in -> content.append(IOUtils.toString(in, StandardCharsets.UTF_8)));
        return content.toString();
    }

    /** KeyValue Replacer Methods -- starts */
    public static String addMapValueToJSON(String json, Map<String, String> map, String @NotNull [] jsonKey, String @NotNull [] newKeyArray) {
        Gson gson = new Gson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        for (int i = 0; i < jsonKey.length; i++) {

            if (jsonObject.has(jsonKey[i])) {
                String replacementValue = jsonObject.get(jsonKey[i]).getAsString();
                String mapValue = map.get(replacementValue);
                jsonObject.addProperty(newKeyArray[i], mapValue);
            }
        }
        return gson.toJson(jsonObject);
    }


    /**
     * @param keys
     * @param values
     * @return Map
     */
    public static @NotNull Map<String, String> createReplacementMap(@NotNull String keys, @NotNull String values) {
        Map<String, String> replacementMap = new HashMap<>();

        String[] keyArray = keys.split(",");
        String[] valueArray = values.split(",");

        if (keyArray.length != valueArray.length) {
            throw new IllegalArgumentException("Number of keys and values should be the same.");
        }

        for (int i = 0; i < keyArray.length; i++) {
            replacementMap.put(keyArray[i], valueArray[i]);
        }

        return replacementMap;
    }

    /** KeyValue Replacer Methods -- ends */

    /**
     * @apiNote apply regex pattern to input value
     * @param value
     * @param regexPattern
     * @return String
     */
    public static String applyRegexPattern(String value, String regexPattern) {
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(value);

        if (matcher.find()) {
            return matcher.group();
        }
        return value;
    }
    @Contract(pure = true)
    public static @NotNull String convertToRange(double value) {
        double rangeSize = 10.0;
        int rangeCount = 10;

        int rangeMin = (int) (Math.floor(value * rangeCount) * rangeSize);
        int rangeMax = rangeMin + (int) rangeSize;

        return rangeMin + "-" + rangeMax;
    }


    /**
     * @apiNote convert xml to json
     * @param xml
     * @return JsonObject
     */
    public static @Nullable JsonObject convertXmlToJson(String xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(xml)));

            JsonObject jsonObject = convertElementToJson(document.getDocumentElement());
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @apiNote converts elements to a JsonObject
     * @param element
     * @return JsonObject
     */
    public static @NotNull JsonObject convertElementToJson(@NotNull Element element) {
        JsonObject jsonObject = new JsonObject();

        if (element.hasAttributes()) {
            NamedNodeMap attributes = element.getAttributes();
            for (int i = 0; i < attributes.getLength(); i++) {
                Node attribute = attributes.item(i);
                jsonObject.addProperty(attribute.getNodeName(), attribute.getNodeValue());
            }
        }

        NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element childElement = (Element) node;
                JsonObject childObject = convertElementToJson(childElement);
                jsonObject.add(childElement.getNodeName(), childObject);
            }
        }

        if (element.hasChildNodes() && element.getChildNodes().getLength() == 1 && element.getFirstChild().getNodeType() == Node.TEXT_NODE) {
            jsonObject.addProperty(element.getNodeName(), element.getTextContent());
        }

        return jsonObject;
    }

    /**
     * @apiNote remove null and blank fields from json objects
     * @param jsonObject
     */
    public static void removeNullEmptyAndWhitespaceFields(@NotNull JsonObject jsonObject) {
        Iterator<Map.Entry<String, JsonElement>> iterator = jsonObject.entrySet().iterator();
        List<String> keysToRemove = new ArrayList<>();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonElement> entry = iterator.next();
            String key = entry.getKey();
            JsonElement value = entry.getValue();

            if (value == null || value.isJsonNull() || value.getAsString().trim().isEmpty()) {
                keysToRemove.add(key);
            } else if (value.isJsonObject()) {
                // Recursively remove null, empty, and whitespace fields in nested JSON objects
                removeNullEmptyAndWhitespaceFields(value.getAsJsonObject());
            }
        }

        // Remove the null, empty, and whitespace fields
        for (String key : keysToRemove) {
            jsonObject.remove(key);
        }
    }
}
