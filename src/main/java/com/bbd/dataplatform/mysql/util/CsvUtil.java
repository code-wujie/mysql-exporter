package com.bbd.dataplatform.mysql.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CsvUtil {

    public static String mapToCSVLine(Map<String, Object> map) {
        List<String> row = new ArrayList<>();
        for (Object object : map.values()) {
            if (object == null) {
                row.add("");
            } else if (object instanceof Date) {
                if (object instanceof Time) {
                    Time time = (Time) object;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
                    row.add(simpleDateFormat.format(time));
                } else if (object instanceof Timestamp) {
                    Timestamp timestamp = (Timestamp) object;
                    row.add(String.valueOf((int) (timestamp.getTime()/1000)));
                } else {
                    Date date = (Date) object;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
                    row.add(simpleDateFormat.format(date));
                }
            } else {
                row.add(object.toString());
            }
        }
        return String.join(",", row);
    }
}
