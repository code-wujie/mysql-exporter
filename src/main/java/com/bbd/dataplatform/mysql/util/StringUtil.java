package com.bbd.dataplatform.mysql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
	
	/**
	 * 判断字符串是否为空
	 * @param str	需要判断的字符串
	 * @return	若为空，返回true，否则返回false
	 */
	public static boolean isNull(String str){
		if(str == null){
			return true;
		}

        return str.trim().length() == 0;

    }
	
	
	/**
	 * 判断字符串是否为空
	 * @param str	需要判断的字符串
	 * @return	若为空，返回false，否则返回true
	 */
	public static boolean isNotNull(String str){
		return !isNull(str);
	}
	
	/**
	 * 根据Unicode编码完美的判断中文汉字和符号
	 * @param c	字符
	 * @return	返回是否是中文字符
	 */
	public static boolean isChineseChar(char c) {
        return (c >= 0x4e00) && (c <= 0x9fbb);
	}
	
	/**
	 * 判断字符串中是否含有中文
	 * @param str	字符串
	 * @return	含有中文则返回true
	 */
	public static boolean isHasChinese(String str) {
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (isChineseChar(c)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 判断字符串中是否含有中文
	 * @param str	字符串
	 * @param min	最少出现中文的次数
	 * @return	含有中文则返回true
	 */
	public static boolean isHasChinese(String str, int min) {
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (isChineseChar(c)) {
				min = min -1;
				if(min <= 0){
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * 判断字符串中是否全是中文
	 * @param str	字符串
	 * @return	含有中文则返回true
	 */
	public static boolean isAllChinese(String str) {
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (!isChineseChar(c)) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 判断字符串为中文,但排除others
	 * @param str	字符串
	 * @param others	需要排除的字符串
	 * @return	满足条件返回true
	 */
	public static boolean isAllChinese(String str, char[] others){
		if(isNull(str)){
			return false;
		}
		//去掉排除的字符串
		if(others != null && others.length > 0){
			for (char oc : others) {
				String tmp = String.valueOf(oc);
				str = str.replace(tmp, "");
			}
		}
		//验证
		return isAllChinese(str);
	}
	
	/**
	 * 判断字符串是否含有数字
	 * @param str	字符串
	 * @return	含有数字则返回true
	 */
	public static boolean isHasNumeric(String str){
		for (int i = str.length(); --i>=0;){    
			if (Character.isDigit(str.charAt(i))){  
				return true;  
			}  
		}  
		return false;  
	}
	
	/**
	 * 判断字符串是否含有数字
	 * @param str	字符串
	 * @param min	最少出现数字的次数
	 * @return	含有数字则返回true
	 */
	public static boolean isHasNumeric(String str, int min){
		for (int i = str.length(); --i>=0;){    
			if (Character.isDigit(str.charAt(i))){  
				min = min -1;
				if(min <= 0){
					return true;
				}
			}  
		}  
		return false;  
	}
	
	/**
	 * 判断字符串是否为数字
	 * @param str	字符串
	 * @return	是数字则返回true
	 */
	public static boolean isNumeric(String str){
		for (int i = str.length(); --i>=0;){    
			if (!Character.isDigit(str.charAt(i))){  
				return false;  
			}  
		}  
		return true;  
	}
	
	/**
	 * 判断字符串是否为固定长度的数字
	 * @param str	字符串
	 * @param length	约束的长度
	 * @return	满足条件返回true
	 */
	public static boolean isNumeric(String str, int length ){
        return isNumeric(str) && str.length() == length;
	}
	
	/**
	 * 判断字符串是否为字母
	 * @param str	字符串
	 * @return	是数字则返回true
	 */
	public static boolean isLetter(String str){
		if(isNull(str)){
			return false;  
		}
        return cheackByReg(str, "^([a-z]|[A-Z])+$");
	}
	
	/**
	 * 判断字符串是否为字母
	 * @param str	字符串
	 * @return	是数字则返回true
	 */
	public static boolean isLetter(String str, int min){
		if(isNull(str)){
			return false;  
		}
		//判断是否为字母
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			if (isLetter(String.valueOf(ch[i]))){
				min = min -1;
				if(min <= 0){
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * 判断字符串为中文或数字的组合
	 * @param str	字符串
	 * @return	满足条件返回true
	 */
	public static boolean isChineseOrNumeric(String str){
		if(isNull(str)){
			return false;
		}
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (!Character.isDigit(c) && !isChineseChar(c)){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 判断字符串为中文或数字的组合,但排除others
	 * @param str	字符串
	 * @param others	需要排除的字符串
	 * @return	满足条件返回true
	 */
	public static boolean isChineseOrNumeric(String str, char[] others){
		if(isNull(str)){
			return false;
		}
		//去掉排除的字符串
		if(others != null && others.length > 0){
			for (char oc : others) {
				String tmp = String.valueOf(oc);
				str = str.replace(tmp, "");
			}
		}
		//验证
		return isChineseOrNumeric(str);
	}
	
	/**
	 * 判断字符串为中文或字母的组合
	 * @param str	字符串
	 * @return	满足条件返回true
	 */
	public static boolean isChineseOrLetter(String str){
		if(isNull(str)){
			return false;
		}
		//定义临时变量
		int isLetterNum = 0;
		int isChineseNum = 0;
		//判断是否为字母与数字
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (isLetter(String.valueOf(c))){
				isLetterNum += 1;
			}
			else if(isChineseChar(c)){
				isChineseNum += 1;
			}
			else{
				return false;
			}
		}
		//排除纯数字与纯中文的情况
        return isLetterNum > 0 && isChineseNum > 0;
	}
	
	/**
	 * 判断字符串为中文或字母的组合,但排除others
	 * @param str	字符串
	 * @param others	需要排除的字符串
	 * @return	满足条件返回true
	 */
	public static boolean isChineseOrLetter(String str, char[] others){
		if(isNull(str)){
			return false;
		}
		//去掉排除的字符串
		if(others != null && others.length > 0){
			for (char oc : others) {
				String tmp = String.valueOf(oc);
				str = str.replace(tmp, "");
			}
		}
		//验证
		return isChineseOrLetter(str);
	}
	
	/**
	 * 判断字符串为字母与数字的组合
	 * @param str	字符串
	 * @return	满足条件返回true
	 */
	public static boolean isLetterOrNumeric(String str){
		if(isNull(str)){
			return false;
		}
		//定义临时变量
		int isLetterNum = 0;
		int isDigitNum = 0;
		//判断是否为字母与数字
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (isLetter(String.valueOf(c))){
				isLetterNum += 1;
			}
			else if(Character.isDigit(c)){
				isDigitNum += 1;
			}
			else{
				return false;
			}
		}
		//排除纯数字与纯中文的情况
        return isLetterNum > 0 && isDigitNum > 0;
	}
	
	/**
	 * 判断字符串为中文或字母的组合,但排除others
	 * @param str	字符串
	 * @param others	需要排除的字符串
	 * @return	满足条件返回true
	 */
	public static boolean isLetterOrNumeric(String str, char[] others){
		if(isNull(str)){
			return false;
		}
		//去掉排除的字符串
		if(others != null && others.length > 0){
			for (char oc : others) {
				String tmp = String.valueOf(oc);
				str = str.replace(tmp, "");
			}
		}
		//验证
		return isLetterOrNumeric(str);
	}
	
	/**
	 * 验证是否为日期格式
	 * @param str	待验证字符串
	 * @param strict 是否严格处理时间
	 * @return	满足条件返回true
	 */
	public static boolean isDate(String str, boolean strict){
		String pattern = null;
		if(cheackByReg(str, "^\\d{4}-\\d{1,2}-\\d{1,2}$")){
			pattern = "yyyy-MM-dd";
		}
		if(cheackByReg(str, "^\\d{4}年\\d{1,2}月\\d{1,2}日$")){
			pattern = "yyyy年MM月dd日";
		}
		if(cheackByReg(str, "^\\d{4}/\\d{1,2}/\\d{1,2}$")){
			pattern = "yyyy/MM/dd";
		}
		if((!strict) && cheackByReg(str, "^\\d{4}\\d{2}\\d{2}$")){
			pattern = "yyyyMMdd";
		}
		if(cheackByReg(str, "^\\d{4}.\\d{1,2}.\\d{1,2}$")){
			str = str.replace('.', '-');
			pattern = "yyyy-MM-dd";
		}
		if(pattern != null){
			SimpleDateFormat sp = new SimpleDateFormat(pattern);
			try {
				sp.parse(str);
			} catch (ParseException e) {
				return false;
			}
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 验证是否为日期格式
	 * @param str	待验证字符串
	 * @return	满足条件返回true
	 */
	public static boolean isDate(String str){
		return isDate(str, false);
	}
	
	/**
	 * 根据正则表达式来判断是否匹配
	 * @param str	字符串
	 * @param reg	正则表达式
	 * @return	返回是否匹配
	 */
	public static boolean cheackByReg(String str, String reg){
		Pattern pattern = Pattern.compile(reg,Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(str);
		return matcher.matches();
	}
	
	/**
	 * 包含
	 * @param str 当前字符串
	 * @param keys	需要包含的key
	 * @return	若str中包含keys中的任意项，则返回true，否则返回false.
	 */
	public static boolean isContains(String str, String[] keys){
		for (String key : keys){
			if (str.contains(key)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 字符串对比
	 * @param str1	str1
	 * @param str2	str2
	 * @return	是否相同
	 */
	public static boolean equals(String str1, String str2){
		if(str1 == null && str2 == null){
			return true;
		}
		if(str1 != null){
			return str1.equals(str2);
		}
		else{
			return str2.equals(str1);
		}
	}
	
	/**
	 * 去掉字符串中括号中的内容，包含括号
	 * @param str	待处理的字符串
	 * @return	处理以后的字符串
	 */
	public static String replesInBracketsContent(String str){
		if(str == null){
			return null;
		}
		return str.replaceAll("\\（.*?\\）", "").replaceAll("\\(.*?\\)", "")
				.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
				.trim();
	}
	
	/**
	 * 去掉字符串中括号中的内容，包含括号
	 * @param str	待处理的字符串
	 * @return	处理以后的字符串
	 */
	public static String replesNumeric(String str){
		if(str == null){
			return null;
		}
		return str.replaceAll("\\d+", "").trim();
	}

	public static String removeNonBmpUnicode(String str) {
		if (str == null) {
			return null;
		}
		str = str.replaceAll("[^\\u0000-\\uFFFF]", "");
		return str;
	}
}
