package util;


import org.apache.commons.io.FileUtils;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import org.json.JSONObject;
import java.nio.file.Paths;

public class Settings {
    private JSONObject jsonObject;
    public static String readFileAsString(String file)throws Exception
    {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
    private String fileName = "C:\\Users\\quang.tranchi\\IdeaProjects\\HbaseDDTB\\src\\main\\java\\settings.json";
    public Settings() {
        try{
            String json = new String(Files.readAllBytes(Paths.get(fileName)));
            jsonObject= new JSONObject(json);
            JSONObject jsonObjec1 = new JSONObject(jsonObject.get("Hbase"));
            System.out.println(jsonObjec1);
//            jsonObject = jsonObject.get("Hbase");
//            System.out.println();
        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    public static void main(String[] args) {
        new Settings();
    }

}
