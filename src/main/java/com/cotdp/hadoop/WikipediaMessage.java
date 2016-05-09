package com.cotdp.hadoop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shona on 5/3/16.
 */
public class WikipediaMessage {

    public enum Type {
        EDIT, SPECIAL, TALK
    }

    public Type type;

    public String title;
    public String user;
    public String diffUrl;
    public int byteDiff;
    public String summary;

    public boolean isMinor;
    public boolean isNew;
    public boolean isUnpatrolled;
    public boolean isBotEdit;

    @Override
    public String toString() {
        String flagString = "";
        if (isNew) flagString += "NEW ";
        if (isMinor) flagString += "MINOR ";
        if (isBotEdit) flagString += "BOT ";
        if (isUnpatrolled) flagString += "unpatrolled ";
        return String.format("[%s%s by %s] SUMMARY:%s TITLE: %s, DIFF[%d]: %s "
                , flagString, type, user, summary, title, byteDiff, diffUrl);
    }


    public static boolean filterNonNull(String key, WikipediaMessage value) {
        return key != null && value != null;
    }

    private static WikipediaMessage parseText(String raw) {
        Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
        Matcher m = p.matcher(raw);

        if (!m.find()) {
            throw new IllegalArgumentException("Could not parse message: " + raw);
        } else if (m.groupCount() != 6) {
            throw new IllegalArgumentException("Unexpected parser group count: " + m.groupCount());
        } else {
            WikipediaMessage result = new WikipediaMessage();

            result.title = m.group(1);
            String flags = m.group(2);
            result.diffUrl = m.group(3);
            result.user = m.group(4);
            result.byteDiff = Integer.parseInt(m.group(5));
            result.summary = m.group(6);

            result.isNew = flags.contains("N");
            result.isMinor = flags.contains("M");
            result.isUnpatrolled = flags.contains("!");
            result.isBotEdit = flags.contains("B");

            result.type = result.title.startsWith("Special:") ? Type.SPECIAL :
                    (result.title.startsWith("Talk:") ? Type.TALK : Type.EDIT);
            return result;
        }
    }

    public static void main(String[] args) {
        String line = "[[07Template:ISO 3166 code Egypt Ad Daqahliyah Governorate14]]4 N10 02https://en.wikipedia.org/w/index.php?oldid=699135054&rcid=787071233 5* 03Rich Farmbrough 5* (+56) 10[[WP:AES|←]]Redirected page to [[Template:ISO 3166 code Egypt Ad Daqahliyah]]";

        System.out.print(parseText(line).toString());
    }

}