package com.fake.dataworks.exception;

public class StudioException extends RuntimeException {
    private final String code;
    private final int status;
    public StudioException(String code, String message, int status) { super(message); this.code=code; this.status=status; }
    public String code() { return code; }
    public int status() { return status; }
    public static StudioException bad(String code,String message) { return new StudioException(code,message,400); }
    public static StudioException conflict(String code,String message) { return new StudioException(code,message,409); }
    public static StudioException missing(String message) { return new StudioException("NOT_FOUND",message,404); }
}
