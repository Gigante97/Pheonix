import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class GenerateArray {
    private static String[] myList = new String[1000];
    public String[] generateArrays() throws FileNotFoundException {
        File file = new File("array_test.txt");
        Scanner scanner = new Scanner(file);
        int i =0;
        while (scanner.hasNextLine()){
            myList[i]=scanner.nextLine();
            i++;
        }

        return myList;
    }


    public static String getElementArray (int o) {
        return myList[o];
    }
}
