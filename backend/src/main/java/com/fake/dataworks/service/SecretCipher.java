package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SecretCipher {
    private final String key;
    public SecretCipher(@Value("${studio.datasource.encryption-key:}") String key) { this.key=key; }
    private Cipher cipher(int mode, byte[] nonce) throws Exception {
        byte[] decoded=Base64.getDecoder().decode(key);
        if(decoded.length!=32) throw new IllegalArgumentException();
        Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode,new SecretKeySpec(decoded,"AES"),new GCMParameterSpec(128,nonce));
        return cipher;
    }
    public String encrypt(String password) {
        try {
            byte[] nonce=new byte[12];new SecureRandom().nextBytes(nonce);
            return Base64.getEncoder().encodeToString(nonce)+":"+Base64.getEncoder().encodeToString(cipher(Cipher.ENCRYPT_MODE,nonce).doFinal(password.getBytes(StandardCharsets.UTF_8)));
        } catch(Exception e) { throw new StudioException("CREDENTIAL_KEY_UNAVAILABLE","数据源加密密钥未配置或无效，请执行业务库初始化脚本",503); }
    }
    public String decrypt(String encrypted) {
        try {
            String[] pieces=encrypted.split(":",2);
            return new String(cipher(Cipher.DECRYPT_MODE,Base64.getDecoder().decode(pieces[0])).doFinal(Base64.getDecoder().decode(pieces[1])),StandardCharsets.UTF_8);
        } catch(Exception e) { throw new StudioException("CREDENTIAL_KEY_UNAVAILABLE","数据源凭据无法解密，请检查原加密密钥",503); }
    }
}
