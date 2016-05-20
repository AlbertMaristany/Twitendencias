package Streaming;

import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.LanguageProfile;

public class LanguageDetector {
	
	static boolean isSpanish(String s) {
		LanguageIdentifier identifier = new LanguageIdentifier(s);
		return identifier.getLanguage().equals("es");
	}
	
	static boolean isCatalan(String s) {
		LanguageIdentifier identifier = new LanguageIdentifier(s);
		return identifier.getLanguage().equals("ca");
	}
	
}
