package com.fake.dataworks.service;

import java.util.*;

/** The prebuilt inventory contract. User SELECTs must return these named columns. */
public final class InventorySql {
    private InventorySql() {}
    public static final String DWD="dwd_inventory_ledger_di",DWS="dws_inventory_warehouse_di",ADS="ads_inventory_analysis_di";
    public static final List<String> TARGETS=List.of(DWD,DWS,ADS);
    public static final List<String> SOURCES=List.of("opening","inbound","outbound","transfer","adjustment");
    public static final String METRICS="opening_qty,inbound_qty,outbound_qty,transfer_in_qty,transfer_out_qty,adjustment_qty,closing_qty,opening_amount,closing_amount,outbound_amount,sku_count,negative_sku_count,low_sku_count";
    public static final Map<String,String> COLUMNS=Map.of(
        DWD,"business_date,row_key,row_kind,movement_type,source_table,source_doc,warehouse_id,sku_id,quantity,unit_cost,inventory_amount",
        DWS,"business_date,warehouse_id,"+METRICS,
        ADS,"business_date,warehouse_id,warehouse_name,region,"+METRICS+",net_change_qty,low_stock_ratio,anomaly");
    public static String rawCte() {
        return "WITH raw_events AS ("+String.join(" UNION ALL ",SOURCES.stream().map(s->"SELECT '"+s.toUpperCase(Locale.ROOT)+"' AS source_kind, e.* FROM ods_inventory_"+s+" e WHERE ingested_at <= :source_cutoff").toList())+"), ranked AS (SELECT e.*,ROW_NUMBER() OVER (PARTITION BY source_kind,document_no,line_no ORDER BY revision DESC,ingested_at DESC,record_id DESC) AS rn FROM raw_events e), valid_events AS (SELECT * FROM ranked WHERE rn=1 AND status='VALID') ";
    }
    public static String validation() {
        return rawCte()+"SELECT COUNT(*) AS invalid_count FROM valid_events e LEFT JOIN dim_warehouse w ON w.warehouse_id=e.warehouse_id LEFT JOIN dim_sku s ON s.sku_id=e.sku_id LEFT JOIN dim_warehouse t ON t.warehouse_id=e.to_warehouse_id WHERE e.business_date<=:bizdate AND (w.warehouse_id IS NULL OR s.sku_id IS NULL OR e.quantity IS NULL OR s.standard_cost<0 OR s.safety_stock<0 OR (e.source_kind<>'ADJUSTMENT' AND e.quantity<0) OR (e.source_kind='TRANSFER' AND (t.warehouse_id IS NULL OR e.warehouse_id=e.to_warehouse_id)))";
    }
    public static String dwd() {
        return rawCte()+"""
            , legs AS (
              SELECT source_kind,document_no,line_no,business_date,warehouse_id,sku_id,
                CASE WHEN source_kind IN ('OUTBOUND','TRANSFER') THEN -quantity ELSE quantity END AS quantity,
                CASE WHEN source_kind='TRANSFER' THEN 'TRANSFER_OUT' ELSE source_kind END AS movement_type
              FROM valid_events
              UNION ALL
              SELECT source_kind,document_no,line_no,business_date,to_warehouse_id,sku_id,quantity,'TRANSFER_IN'
              FROM valid_events WHERE source_kind='TRANSFER'
            ), positions AS (
              SELECT DISTINCT warehouse_id,sku_id FROM legs WHERE business_date<=:bizdate
            ), ledger AS (
              SELECT CAST(:bizdate AS DATE) AS business_date,
                CONCAT('OPEN:',p.warehouse_id,':',p.sku_id) AS row_key,'OPENING_SNAPSHOT' AS row_kind,
                'OPENING' AS movement_type,'opening_and_prior_flows' AS source_table,'DAY_OPEN' AS source_doc,
                p.warehouse_id,p.sku_id,COALESCE(SUM(l.quantity),0) AS quantity
              FROM positions p LEFT JOIN legs l ON l.warehouse_id=p.warehouse_id AND l.sku_id=p.sku_id
                AND (l.business_date<:bizdate OR (l.source_kind='OPENING' AND l.business_date<=:bizdate))
              GROUP BY p.warehouse_id,p.sku_id
              UNION ALL
              SELECT CAST(:bizdate AS DATE),CONCAT(l.source_kind,':',l.document_no,':',l.line_no,':',l.movement_type),
                'MOVEMENT',l.movement_type,CONCAT('ods_inventory_',LOWER(l.source_kind)),l.document_no,
                l.warehouse_id,l.sku_id,l.quantity FROM legs l WHERE l.business_date=:bizdate AND l.source_kind<>'OPENING'
            )
            SELECT l.*,s.standard_cost AS unit_cost,l.quantity*s.standard_cost AS inventory_amount
            FROM ledger l JOIN dim_sku s ON s.sku_id=l.sku_id
            """;
    }
    public static String dws() {
        return """
            WITH sku_balances AS (
              SELECT l.business_date,l.warehouse_id,l.sku_id,
                SUM(CASE WHEN movement_type='OPENING' THEN quantity ELSE 0 END) AS opening_qty,
                SUM(CASE WHEN movement_type='INBOUND' THEN quantity ELSE 0 END) AS inbound_qty,
                -SUM(CASE WHEN movement_type='OUTBOUND' THEN quantity ELSE 0 END) AS outbound_qty,
                SUM(CASE WHEN movement_type='TRANSFER_IN' THEN quantity ELSE 0 END) AS transfer_in_qty,
                -SUM(CASE WHEN movement_type='TRANSFER_OUT' THEN quantity ELSE 0 END) AS transfer_out_qty,
                SUM(CASE WHEN movement_type='ADJUSTMENT' THEN quantity ELSE 0 END) AS adjustment_qty,
                SUM(quantity) AS closing_qty,
                SUM(CASE WHEN movement_type='OPENING' THEN inventory_amount ELSE 0 END) AS opening_amount,
                SUM(inventory_amount) AS closing_amount,
                -SUM(CASE WHEN movement_type='OUTBOUND' THEN inventory_amount ELSE 0 END) AS outbound_amount
              FROM etl_stage_dwd_inventory_ledger_di l WHERE build_id=:build_id AND business_date=:bizdate
              GROUP BY business_date,warehouse_id,sku_id
            )
            SELECT b.business_date,b.warehouse_id,SUM(opening_qty) AS opening_qty,SUM(inbound_qty) AS inbound_qty,
              SUM(outbound_qty) AS outbound_qty,SUM(transfer_in_qty) AS transfer_in_qty,SUM(transfer_out_qty) AS transfer_out_qty,
              SUM(adjustment_qty) AS adjustment_qty,SUM(closing_qty) AS closing_qty,SUM(opening_amount) AS opening_amount,
              SUM(closing_amount) AS closing_amount,SUM(outbound_amount) AS outbound_amount,COUNT(*) AS sku_count,
              SUM(CASE WHEN closing_qty<0 THEN 1 ELSE 0 END) AS negative_sku_count,
              SUM(CASE WHEN closing_qty>=0 AND closing_qty<s.safety_stock THEN 1 ELSE 0 END) AS low_sku_count
            FROM sku_balances b JOIN dim_sku s ON s.sku_id=b.sku_id GROUP BY b.business_date,b.warehouse_id
            """;
    }
    public static String ads() {
        return "SELECT d.business_date,d.warehouse_id,w.warehouse_name,w.region,"+
            String.join(",",Arrays.stream(METRICS.split(",")).map(s->"d."+s).toList())+
            ",d.closing_qty-d.opening_qty AS net_change_qty,CASE WHEN d.sku_count=0 THEN 0 ELSE d.low_sku_count/d.sku_count END AS low_stock_ratio,"+
            "CASE WHEN d.negative_sku_count>0 THEN 'NEGATIVE_STOCK' WHEN d.low_sku_count>0 THEN 'LOW_STOCK' ELSE 'NORMAL' END AS anomaly "+
            "FROM etl_stage_dws_inventory_warehouse_di d JOIN dim_warehouse w ON w.warehouse_id=d.warehouse_id WHERE d.build_id=:build_id AND d.business_date=:bizdate";
    }
}
