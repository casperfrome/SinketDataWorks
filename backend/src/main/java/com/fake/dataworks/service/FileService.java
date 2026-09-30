package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.domain.StudioObject;
import com.fake.dataworks.dto.ObjectInput;
import com.fake.dataworks.repository.StudioRepository;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import org.springframework.core.io.Resource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class FileService {
    private static final Logger log=LoggerFactory.getLogger(FileService.class);
    private final Path storage; private final StudioRepository repo; private final ObjectService objects;
    public FileService(@Value("${studio.storage-path:./storage}") String storage,StudioRepository repo,ObjectService objects) {this.storage=Path.of(storage).toAbsolutePath().normalize();this.repo=repo;this.objects=objects;}
    @Transactional(rollbackFor=Exception.class)
    public StudioObject upload(String id,MultipartFile file) throws IOException {
        var initial=objects.active(id);repo.lockWorkspace(initial.workspaceId());var o=objects.active(id);if(!Set.of("RESOURCE","FUNCTION","COMPONENT").contains(o.kind())) throw StudioException.bad("INVALID_RESOURCE","只有资源、函数或组件可以上传文件");
        if(file.isEmpty()) throw StudioException.bad("EMPTY_FILE","文件不能为空");
        Files.createDirectories(storage);String stored=UUID.randomUUID().toString();Path target=storage.resolve(stored);String name=Objects.toString(file.getOriginalFilename(),"resource.bin").replace('\\','/');name=name.substring(name.lastIndexOf('/')+1);if(name.isBlank()||name.length()>255) name="resource.bin";
        String type=Objects.toString(file.getContentType(),"application/octet-stream");var old=repo.file(id);
        try {file.transferTo(target);}catch(Exception e){Files.deleteIfExists(target);throw e;}
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCompletion(int status) {
                try {
                    if(status!=STATUS_COMMITTED) Files.deleteIfExists(target);
                    else if(old.isPresent()&&!repo.storageReferenced(old.get().get("storageName").toString())) Files.deleteIfExists(safePath(old.get().get("storageName").toString()));
                } catch(Exception cleanupFailure) {log.warn("Resource storage cleanup deferred; object id={}",id);}
            }
        });
        repo.file(id,stored,name,type,file.getSize());Map<String,Object> config=new LinkedHashMap<>(o.config());config.put("fileName",name);config.put("file",Map.of("name",name,"size",file.getSize(),"contentType",type,"uploadedAt",ObjectService.now()));
        return objects.update(id,new ObjectInput(o.workspaceId(),o.parentId(),o.kind(),o.nodeType(),o.name(),o.description(),o.content(),config,o.tags(),o.favorite(),o.version(),o.owner()));
    }
    public Map<String,Object> metadata(String id) {
        var object=objects.active(id);var stored=repo.file(id);if(stored.isPresent())return stored.get();
        if(Set.of("RESOURCE","FUNCTION","COMPONENT").contains(object.kind())&&!object.content().isEmpty()) return Map.of("name",object.name(),"contentType","text/plain;charset=UTF-8","size",object.content().getBytes(StandardCharsets.UTF_8).length);
        throw StudioException.missing("尚未上传资源文件");
    }
    public Resource download(String id) {
        var metadata=metadata(id);
        if(metadata.containsKey("storageName"))return new FileSystemResource(safePath(metadata.get("storageName").toString()));
        return new ByteArrayResource(objects.active(id).content().getBytes(StandardCharsets.UTF_8));
    }
    public Path safePath(String name) {Path p=storage.resolve(name).normalize();if(!p.startsWith(storage)) throw StudioException.bad("INVALID_PATH","非法资源路径");return p;}
}
