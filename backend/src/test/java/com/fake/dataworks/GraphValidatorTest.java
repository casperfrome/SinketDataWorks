package com.fake.dataworks;

import com.fake.dataworks.exception.StudioException;
import com.fake.dataworks.service.GraphValidator;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GraphValidatorTest {
    private final GraphValidator validator=new GraphValidator();
    private Map<String,Object> graph(List<?> nodes,List<?> edges) {return Map.of("graph",Map.of("nodes",nodes,"edges",edges));}
    private Map<String,Object> node(String id) {return Map.of("id",id);}
    private Map<String,Object> edge(String id,String source,String target) {return Map.of("id",id,"source",source,"target",target);}
    @Test void acceptsDagAndDisconnectedNodes() {assertDoesNotThrow(()->validator.validate(graph(List.of(node("a"),node("b"),node("c")),List.of(edge("e","a","b")))));}
    @Test void rejectsDuplicateIds() {assertThrows(StudioException.class,()->validator.validate(graph(List.of(node("a"),node("a")),List.of())));}
    @Test void rejectsOrphanAndSelfEdges() {
        assertThrows(StudioException.class,()->validator.validate(graph(List.of(node("a")),List.of(edge("e","a","missing")))));
        assertThrows(StudioException.class,()->validator.validate(graph(List.of(node("a")),List.of(edge("e","a","a")))));
    }
    @Test void rejectsCycles() {assertThrows(StudioException.class,()->validator.validate(graph(List.of(node("a"),node("b")),List.of(edge("e1","a","b"),edge("e2","b","a")))));}
    @Test void rejectsDuplicateConnectionsEvenWithDifferentEdgeIds() {assertThrows(StudioException.class,()->validator.validate(graph(List.of(node("a"),node("b")),List.of(edge("e1","a","b"),edge("e2","a","b")))));}
    @Test void validatesTwelveThousandNodeChainWithoutUsingCallStack() {
        List<Object> nodes=new ArrayList<>(),edges=new ArrayList<>();
        for(int i=0;i<12000;i++){nodes.add(node("n"+i));if(i>0)edges.add(edge("e"+i,"n"+(i-1),"n"+i));}
        assertDoesNotThrow(()->validator.validate(graph(nodes,edges)));
        edges.add(edge("back","n11999","n0"));assertThrows(StudioException.class,()->validator.validate(graph(nodes,edges)));
    }
}
