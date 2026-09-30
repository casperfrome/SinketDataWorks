package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;
import org.springframework.stereotype.Component;

@Component
public class GraphValidator {
    public void validate(Map<String,Object> config) {
        if(!config.containsKey("graph")) return;
        if(!(config.get("graph") instanceof Map<?,?>)) fail("工作流 graph 必须为对象");
        Map<?,?> graph=(Map<?,?>)config.get("graph");
        if(!(graph.get("nodes") instanceof List<?>) || !(graph.get("edges") instanceof List<?>)) fail("工作流必须包含 nodes 和 edges 数组");
        List<?> nodes=(List<?>)graph.get("nodes"), edges=(List<?>)graph.get("edges");
        Set<String> ids=new HashSet<>(), edgeIds=new HashSet<>(); Map<String,Set<String>> next=new HashMap<>();Map<String,Integer> incoming=new HashMap<>();
        for(Object item:nodes) {
            if(!(item instanceof Map<?,?>)) fail("节点格式不正确");
            String id=Objects.toString(((Map<?,?>)item).get("id"),"");
            if(id.isBlank() || !ids.add(id)) fail("节点 ID 不能为空或重复");
            next.put(id,new HashSet<>());incoming.put(id,0);
        }
        for(Object item:edges) {
            if(!(item instanceof Map<?,?>)) fail("连线格式不正确");
            Map<?,?> edge=(Map<?,?>)item; String id=Objects.toString(edge.get("id"),""), source=Objects.toString(edge.get("source"),""),target=Objects.toString(edge.get("target"),"");
            if(id.isBlank() || !edgeIds.add(id)) fail("连线 ID 不能为空或重复");
            if(!ids.contains(source)||!ids.contains(target)) fail("连线引用了不存在的节点");
            if(source.equals(target)) fail("节点不能连接自身");
            if(!next.get(source).add(target)) fail("相同的起点和终点不能重复连线");
            incoming.merge(target,1,Integer::sum);
        }
        // Kahn's algorithm avoids stack overflow on deep workflows.
        Deque<String> ready=new ArrayDeque<>();incoming.forEach((id,count)->{if(count==0)ready.add(id);});int processed=0;
        while(!ready.isEmpty()) {String id=ready.removeFirst();processed++;for(String target:next.get(id)) if(incoming.merge(target,-1,Integer::sum)==0)ready.addLast(target);}
        if(processed!=ids.size())fail("工作流存在循环依赖");
    }
    private void fail(String message) { throw StudioException.bad("INVALID_GRAPH",message); }
}
