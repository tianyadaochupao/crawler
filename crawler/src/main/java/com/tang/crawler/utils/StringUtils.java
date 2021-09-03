package com.tang.crawler.utils;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class StringUtils {

    public static List<String> spiltStrToList(String content){

        List<String> lineList = new ArrayList<String>();
        StringReader reader =null;
        BufferedReader rdr=null;
        try{
            reader = new StringReader(content);
            rdr = new BufferedReader(reader);
            String line=null;
            while (null!=(line=rdr.readLine())){
                lineList.add(line);
            }
        }catch (Exception e){

        }finally {
            if(null!=reader){
                reader.close();
            }
            if(null!=rdr){
                try {
                    rdr.close();
                }catch (Exception e){
                }
            }
        }
        return lineList;
    }




}
