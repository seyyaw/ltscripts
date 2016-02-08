package de.tudarmstadt.lt.nod;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import org.apache.commons.io.IOUtils;

public class DIVIDEntityToNonEntity {

    public static void main(String[] args)
    {
        try {
            File fileDir = new File(args[0]);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(fileDir), "UTF8"));
            FileOutputStream os = new FileOutputStream(
                    args[1]);
            String str;
            int i = 0;
            while ((str = in.readLine()) != null) {
                i++;
                if (i % 10000 == 0) {
                    System.out.println(i);
                }
                if(str.split("\t")[1].equals("PER")){
                	 IOUtils.write( str.split("\t")[0] + "\t1\t"+str.split("\t")[2] +"\t" + str.split("\t")[3]+"\n" , os, "UTF-8");
                }
                else if (str.split("\t")[1].equals("ORG")){
                	 IOUtils.write( str.split("\t")[0] + "\t2\t"+str.split("\t")[2] +"\t" + str.split("\t")[3]+"\n" , os, "UTF-8");
                }
            }
            in.close();
            os.close();
        }
        catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
