package Streaming;


import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class SportWords implements Serializable
{
    public static final long serialVersionUID = 42L;
    private Set<String> sportWords;
    private static SportWords _singleton;

    private SportWords()
    {
        this.sportWords = new HashSet<String>();
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(
                new InputStreamReader(
                    this.getClass().getResourceAsStream("/sports-words.txt")));
            String line;
            while ((line = rd.readLine()) != null)
                this.sportWords.add(line);
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

    private static SportWords get()
    {
        if (_singleton == null)
            _singleton = new SportWords();
        return _singleton;
    }

    public static Set<String> getWords()
    {
        return get().sportWords;
    }
}