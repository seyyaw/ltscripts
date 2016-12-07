package scripts;

import java.util.regex.Pattern;
import java.util.stream.IntStream;


public class PatternTest {

	public static void main(String[] args) {
		String pattern = ".*Ordner\\s+\\d+\\s+von\\s+\\d+" ;
		
		System.out.println(Pattern.matches(pattern, "This Ordner 3 von 456"));
		String text = "this is the index of the think the one ";

	}
}
