package com.fake.dataworks.exception;

import java.util.Map;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(StudioException.class)
    ResponseEntity<?> studio(StudioException e) { return ResponseEntity.status(e.status()).body(Map.of("code",e.code(),"message",e.getMessage())); }
    @ExceptionHandler(DuplicateKeyException.class)
    ResponseEntity<?> duplicate(DuplicateKeyException e) { return ResponseEntity.status(409).body(Map.of("code","NAME_CONFLICT","message","同一目录下已存在同名对象")); }
    @ExceptionHandler({HttpMessageNotReadableException.class,IllegalArgumentException.class})
    ResponseEntity<?> invalid(Exception e) { return ResponseEntity.badRequest().body(Map.of("code","INVALID_REQUEST","message","请求格式不正确，请检查字段及类型")); }
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    ResponseEntity<?> size(Exception e) { return ResponseEntity.status(413).body(Map.of("code","FILE_TOO_LARGE","message","文件不能超过 20 MB")); }
    @ExceptionHandler(Exception.class)
    ResponseEntity<?> unexpected(Exception e) { return ResponseEntity.internalServerError().body(Map.of("code","INTERNAL_ERROR","message","操作失败，请检查本地服务与数据库连接")); }
}
