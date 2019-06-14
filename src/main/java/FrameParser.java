
import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

//created by Fabrice Medio
//from https://git.soma.salesforce.com/code-eye/code/blob/master/src/main/java/com/salesforce/javaparser/FrameParser.java

public class FrameParser implements Iterator<JsonObject> {
    private JsonObject currentFrame;
    private boolean isDirty;
    private BufferedReader reader;
    private JsonParser jsonParser;

    public FrameParser(InputStream inputStream) {
        isDirty = true;
        reader = new BufferedReader(new InputStreamReader(inputStream));
        jsonParser = new JsonParser();
    }

    @Override
    public boolean hasNext() {
        try {
            refresh();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return currentFrame != null;
    }

    @Override
    public JsonObject next() {
        try {
            refresh();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (currentFrame == null) {
            throw new NoSuchElementException();
        }
        isDirty = true;
        JsonObject result = currentFrame;
        currentFrame = null;
        return result;
    }

    enum ParserState {
        initial,
        parsingFrame
    }

    private void refresh() throws IOException {
        if (isDirty) {
            int nestingLevel = 0;
            ParserState state = ParserState.initial;
            StringBuilder sb = new StringBuilder();

            for (int c = reader.read(); c != -1; c = reader.read()) {
                if (state.equals(ParserState.initial) && c == '{') {
                    sb.append((char) c);
                    state = ParserState.parsingFrame;
                    nestingLevel++;
                } else if (state.equals(ParserState.parsingFrame)) {
                    sb.append((char) c);

                    if (c == '{') {
                        nestingLevel++;
                    } else if (c == '}') {
                        nestingLevel--;
                    }

                    if (nestingLevel == 0) {
                        currentFrame = (JsonObject) jsonParser.parse(sb.toString());
                        isDirty = false;
                        return;
                    }
                }
            }
        }
    }
}
