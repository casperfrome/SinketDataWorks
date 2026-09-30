package com.fake.dataworks.service;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SyncPartitionMetadataTest {
    private static final List<Map<String,Object>> COLUMNS=List.of(Map.of("name","ds","type","date"),Map.of("name","id","type","bigint"));
    @Test void emptyAutoTableRetainsItsDatePartitionDefinition(){
        var metadata=SyncPartitionMetadata.parse("CREATE TABLE t(ds DATE NOT NULL,id BIGINT) DUPLICATE KEY(ds,id) AUTO PARTITION BY RANGE (date_trunc(`ds`, 'day')) () DISTRIBUTED BY HASH(id) BUCKETS AUTO",COLUMNS,List.of());
        assertEquals("RANGE",metadata.type());assertTrue(metadata.automatic());assertEquals("date_trunc(`ds`, 'day')",metadata.expression());
        assertEquals(List.of(new SyncPartitionMetadata.Column("ds","date")),metadata.columns());assertTrue(metadata.partitions().isEmpty());assertTrue(metadata.matching(Map.of("ds","2026-09-30")).isEmpty());
    }
    @Test void actualRangeBoundsResolveANameUnrelatedToTheDate(){
        var metadata=SyncPartitionMetadata.parse("PARTITION BY RANGE (`ds`) (PARTITION historical VALUES [('2026-09-29'), ('2026-09-30'))) ",COLUMNS,List.of(Map.of("PartitionName","arbitrary_name","Range","[types: [DATEV2]; keys: [2026-09-29]; ..types: [DATEV2]; keys: [2026-09-30]; )")));
        assertEquals(List.of("arbitrary_name"),metadata.matching(Map.of("ds","2026-09-29")));assertTrue(metadata.matching(Map.of("ds","2026-09-30")).isEmpty());
        assertEquals("2026-09-29",metadata.partitions().getFirst().view().get("lower"));
    }
    @Test void unpartitionedInternalTabletIsNotShownAsAUserPartition(){
        var metadata=SyncPartitionMetadata.parse("CREATE TABLE t(ds DATE,id BIGINT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1",COLUMNS,List.of(Map.of("PartitionName","t","Range","")));
        assertEquals("NONE",metadata.type());assertTrue(metadata.columns().isEmpty());assertTrue(metadata.partitions().isEmpty());assertEquals(metadata,SyncPartitionMetadata.from(Map.of("partition",metadata.view())));
    }
    @Test void listMetadataRetainsAllNativeValues(){
        var metadata=SyncPartitionMetadata.parse("PARTITION BY LIST (`region`) ()",List.of(Map.of("name","region","type","varchar(20)")),List.of(Map.of("PartitionName","east","Range","(types: [VARCHAR]; keys: [Shanghai]; )(types: [VARCHAR]; keys: [Hangzhou]; )")));
        assertEquals(List.of("Shanghai","Hangzhou"),metadata.partitions().getFirst().values());assertEquals(List.of("east"),metadata.matching(Map.of("region","Hangzhou")));
    }
    @Test void quotedCommentsCannotInventAPartitionDefinition(){
        var metadata=SyncPartitionMetadata.parse("CREATE TABLE t(ds DATE COMMENT 'PARTITION BY RANGE (not_a_column)',id BIGINT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1",COLUMNS,List.of());
        assertEquals("NONE",metadata.type());
    }
    private static final List<Map<String,Object>> COMPOSITE_COLUMNS=List.of(Map.of("name","ds","type","date"),Map.of("name","zone","type","varchar(20)"),Map.of("name","id","type","bigint"));
    @Test void compositeListKeysFollowExpressionOrderRatherThanTableColumnOrder(){
        var metadata=SyncPartitionMetadata.parse("AUTO PARTITION BY LIST (`zone`,`ds`) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","peast420262d092d2910","Range","[types: [VARCHAR, DATEV2]; keys: [east, 2026-09-29]; ]"),Map.of("PartitionName","west","Range","[types: [VARCHAR, DATEV2]; keys: [west, 2026-09-29]; ]")));
        assertEquals(List.of("zone","ds"),metadata.columns().stream().map(SyncPartitionMetadata.Column::name).toList());assertEquals(List.of("peast420262d092d2910"),metadata.matching(Map.of("ds","2026-09-29","zone","east")));assertTrue(metadata.matching(Map.of("ds","2026-09-30","zone","east")).isEmpty());
        assertEquals(metadata.matching(Map.of("ds","2026-09-29","zone","east")),SyncPartitionMetadata.from(Map.of("partition",metadata.view())).matching(Map.of("ds","2026-09-29","zone","east")));
    }
    @Test void nativeCompositeKeysPreserveLiteralQuotesAndBackslashesWithoutSqlDecoding(){
        var metadata=SyncPartitionMetadata.parse("PARTITION BY LIST (zone,ds) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","quoted","Range","[types: [VARCHAR, DATEV2]; keys: ['foo', 2026-09-29]; ][types: [VARCHAR, DATEV2]; keys: [O'Reilly\\north, 2026-09-29]; ]")));
        assertEquals(List.of("quoted"),metadata.matching(Map.of("zone","'foo'","ds","2026-09-29")));assertEquals(List.of("quoted"),metadata.matching(Map.of("zone","O'Reilly\\north","ds","2026-09-29")));assertTrue(metadata.matching(Map.of("zone","foo","ds","2026-09-29")).isEmpty());assertTrue(metadata.matching(Map.of("zone","O'Reilly\nnorth","ds","2026-09-29")).isEmpty());
    }
    @Test void compositeRangeUsesLexicographicTypedTuplesAndExclusiveUpperBound(){
        var metadata=SyncPartitionMetadata.parse("PARTITION BY RANGE(zone,id) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","east2to10","Range","[types: [VARCHAR, BIGINT]; keys: [east, 2]; ..types: [VARCHAR, BIGINT]; keys: [east, 10]; )")));
        assertEquals(List.of("zone","id"),metadata.columns().stream().map(SyncPartitionMetadata.Column::name).toList());assertEquals(List.of("east2to10"),metadata.matching(Map.of("id","9","zone","east")));assertEquals(List.of("east2to10"),metadata.matching(Map.of("id","2","zone","east")));assertTrue(metadata.matching(Map.of("id","10","zone","east")).isEmpty());assertTrue(metadata.matching(Map.of("id","5","zone","west")).isEmpty());
    }
    @Test void ambiguousNativeCompositeValuesAreNeverGuessed(){
        var metadata=SyncPartitionMetadata.parse("AUTO PARTITION BY LIST(zone,ds) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","ambiguous","Range","[types: [VARCHAR, DATEV2]; keys: [east, north, 2026-09-29]; ]")));
        assertEquals("SYNC_PARTITION_METADATA",assertThrows(com.fake.dataworks.exception.StudioException.class,()->metadata.matching(Map.of("zone","east, north","ds","2026-09-29"))).code());
    }
    @Test void aSingleNativeStringKeyKeepsItsCommasAndUnquotedApostrophes(){
        var metadata=SyncPartitionMetadata.parse("PARTITION BY LIST(zone) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","names","Range","[types: [VARCHAR]; keys: [O'Reilly, East]; ]")));
        assertEquals(List.of("O'Reilly, East"),metadata.partitions().getFirst().values());assertEquals(List.of("names"),metadata.matching(Map.of("zone","O'Reilly, East")));assertTrue(metadata.matching(Map.of("zone","East")).isEmpty());
    }
    @Test void nativeStringWhitespaceAndNonSeparatorCommasCannotMatchDifferentValues(){
        var single=SyncPartitionMetadata.parse("PARTITION BY LIST(zone) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","spaces","Range","[types: [VARCHAR]; keys: [ foo ]; ]")));
        assertTrue(single.matching(Map.of("zone","foo")).isEmpty());assertEquals(List.of("spaces"),single.matching(Map.of("zone"," foo ")));
        var composite=SyncPartitionMetadata.parse("PARTITION BY LIST(zone,ds) ()",COMPOSITE_COLUMNS,List.of(Map.of("PartitionName","spaces","Range","[types: [VARCHAR, DATEV2]; keys: [ foo , 2026-09-29]; ]"),Map.of("PartitionName","comma","Range","[types: [VARCHAR, DATEV2]; keys: [a,b, 2026-09-29]; ]")));
        assertTrue(composite.matching(Map.of("zone","foo","ds","2026-09-29")).isEmpty());assertEquals(List.of("spaces"),composite.matching(Map.of("zone"," foo ","ds","2026-09-29")));assertEquals(List.of("comma"),composite.matching(Map.of("zone","a,b","ds","2026-09-29")));
    }
}
