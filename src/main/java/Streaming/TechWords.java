package Streaming;


import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class TechWords implements Serializable
{
    public static final long serialVersionUID = 42L;
    private Set<String> techWords;
    private static TechWords _singleton;

    private TechWords()
    {
        this.techWords = new HashSet<String>();
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(
                new InputStreamReader(
                    this.getClass().getResourceAsStream("/tech-words.txt")));
            String line;
            while ((line = rd.readLine()) != null)
                this.techWords.add(line);
        }
        catch (IOException ex)
        {
            Logger.getLogger(this.getClass())
                  .error("IO error while initializing", ex);
        }
        finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }

    private static TechWords get()
    {
        if (_singleton == null)
            _singleton = new TechWords();
        return _singleton;
    }

    public static Set<String> getWords()
    {
        return get().techWords;
    }
}