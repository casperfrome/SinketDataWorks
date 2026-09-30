package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;

/**
 * Explicit Doris grammar for forms not represented by JSqlParser. Every token is
 * consumed and every table reference is checked before the original SQL is sent.
 * Queries and DML use the existing AST validator; parse failures are never forwarded.
 */
final class DorisSqlValidator {
    private DorisSqlValidator() {}
    static String validate(String sql,String database,SqlGuard guard) {
        if(sql.matches("(?is)^(?:SELECT|WITH|INSERT|UPDATE|DELETE|REPLACE)\\b.*"))return SqlScript.validate(sql,database,guard,true);
        var parser=new Parser(sql,database,guard);
        String kind;
        if(parser.take("SHOW")){parser.show();kind="QUERY";}
        else if(parser.take("DESC")||parser.take("DESCRIBE")){parser.take("TABLE");parser.table();parser.take("ALL");kind="QUERY";}
        else if(parser.take("CREATE")){parser.create();kind="DDL";}
        else if(parser.take("ALTER")){parser.alter();kind="DDL";}
        else if(parser.take("DROP")){parser.drop();kind="DDL";}
        else if(parser.take("TRUNCATE")){parser.require("TABLE");parser.table();if(parser.take("PARTITION"))parser.identifiers();kind="DDL";}
        else throw invalid();
        parser.end();return kind;
    }
    private static StudioException invalid(){return StudioException.bad("UNSUPPORTED_SQL","SQL 无法解析，或包含不支持的 Doris 语法；仅允许当前 schema 内的查询、数据和 OLAP 表结构操作");}
    private enum Kind { WORD, IDENTIFIER, STRING, NUMBER, SYMBOL }
    private record Token(String text,Kind kind,int start) {}
    private static final class Parser {
        private final String sql,database;private final SqlGuard guard;private final List<Token> tokens=new ArrayList<>();private int index,typeDepth;
        Parser(String sql,String database,SqlGuard guard){this.sql=sql;this.database=database;this.guard=guard;lex();}
        private void lex() {
            for(int i=0;i<sql.length();) {
                char c=sql.charAt(i);if(Character.isWhitespace(c)){i++;continue;}
                int start=i;
                if(c=='`'||c=='\''||c=='"') {
                    char quote=c;var value=new StringBuilder();i++;boolean closed=false;
                    while(i<sql.length()) {
                        c=sql.charAt(i++);
                        if(c=='\\'&&quote!='`'){if(i==sql.length())throw invalid();value.append(sql.charAt(i++));}
                        else if(c==quote){if(i<sql.length()&&sql.charAt(i)==quote){value.append(quote);i++;}else{closed=true;break;}}
                        else value.append(c);
                    }
                    if(!closed)throw invalid();tokens.add(new Token(value.toString(),quote=='`'?Kind.IDENTIFIER:Kind.STRING,start));
                } else if(Character.isLetter(c)||c=='_') {
                    i++;while(i<sql.length()&&(Character.isLetterOrDigit(sql.charAt(i))||sql.charAt(i)=='_'||sql.charAt(i)=='$'))i++;
                    tokens.add(new Token(sql.substring(start,i),Kind.WORD,start));
                } else if(Character.isDigit(c)) {
                    i++;while(i<sql.length()&&Character.isDigit(sql.charAt(i)))i++;
                    if(i<sql.length()&&sql.charAt(i)=='.'){i++;if(i==sql.length()||!Character.isDigit(sql.charAt(i)))throw invalid();while(i<sql.length()&&Character.isDigit(sql.charAt(i)))i++;}
                    tokens.add(new Token(sql.substring(start,i),Kind.NUMBER,start));
                } else if("(),.=[]<>+-*/%?!|&^~:@".indexOf(c)>=0){tokens.add(new Token(String.valueOf(c),Kind.SYMBOL,start));i++;}
                else throw invalid();
            }
            if(tokens.isEmpty())throw invalid();
        }
        boolean at(String word){return index<tokens.size()&&(tokens.get(index).kind()==Kind.WORD||tokens.get(index).kind()==Kind.SYMBOL)&&tokens.get(index).text().equalsIgnoreCase(word);}
        boolean take(String word){if(!at(word))return false;index++;return true;}
        void require(String word){if(!take(word))throw invalid();}
        void end(){if(index!=tokens.size())throw invalid();}
        String identifier(){if(index==tokens.size()||!Set.of(Kind.WORD,Kind.IDENTIFIER).contains(tokens.get(index).kind()))throw invalid();String name=tokens.get(index++).text();if(name.isEmpty())throw invalid();return name;}
        String string(){if(index==tokens.size()||tokens.get(index).kind()!=Kind.STRING)throw invalid();return tokens.get(index++).text();}
        String number(){if(index==tokens.size()||tokens.get(index).kind()!=Kind.NUMBER)throw invalid();return tokens.get(index++).text();}
        void integer(){if(number().contains("."))throw invalid();}
        void table(){String first=identifier();if(take(".")){if(!first.equalsIgnoreCase(database))throw StudioException.bad("UNSUPPORTED_SQL","只能操作当前数据源配置的业务库");identifier();}if(at("."))throw invalid();}
        void schema(){if(!identifier().equalsIgnoreCase(database))throw StudioException.bad("UNSUPPORTED_SQL","只能操作当前数据源配置的业务库");}
        void identifiers(){require("(");identifier();while(take(","))identifier();require(")");}
        void ifExists(){if(take("IF"))require("EXISTS");}
        void ifNotExists(){if(take("IF")){require("NOT");require("EXISTS");}}
        void show(){
            if(take("CREATE")){if(!take("TABLE")&&!take("VIEW"))throw invalid();table();}
            else if(take("PARTITIONS")){require("FROM");table();}
            else if(take("TEMPORARY")){require("PARTITIONS");require("FROM");table();}
            else if(take("TABLES")){if(take("FROM")||take("IN"))schema();if(take("LIKE"))string();}
            else if(take("COLUMNS")||take("FIELDS")){require("FROM");table();if(take("FROM")||take("IN"))schema();if(take("LIKE"))string();}
            else throw invalid();
        }
        void create(){
            require("TABLE");ifNotExists();table();
            if(take("LIKE")){table();return;}
            if(take("(")){column();while(take(",")){if(at("INDEX"))indexDefinition();else column();}require(")");}
            else if(!at("AS"))throw invalid();
            if(take("ENGINE")){take("=");require("OLAP");}
            if(take("DUPLICATE")||take("UNIQUE")||take("AGGREGATE")){require("KEY");identifiers();}
            if(take("COMMENT"))string();
            boolean auto=take("AUTO");if(auto||at("PARTITION"))partitionBy(auto);
            if(at("DISTRIBUTED"))distribution();
            if(take("PROPERTIES"))properties();
            if(take("AS"))query();
        }
        void query(){if(!at("SELECT")&&!at("WITH"))throw invalid();guard.validateDoris(sql.substring(tokens.get(index).start()),database);index=tokens.size();}
        void column(){
            identifier();type();
            if(take("KEY")){} // Explicit aggregate-table key column.
            if(take("SUM")||take("MIN")||take("MAX")||take("REPLACE")||take("REPLACE_IF_NOT_NULL")||take("HLL_UNION")||take("BITMAP_UNION")||take("QUANTILE_UNION")){}
            if(take("NOT"))require("NULL");else take("NULL");
            if(take("DEFAULT")){if(take("CURRENT_TIMESTAMP")){if(take("(")){integer();require(")");}}else literal();}
            if(take("AUTO_INCREMENT")){if(take("(")){integer();require(")");}}
            if(take("COMMENT"))string();
        }
        void type(){
            if(++typeDepth>32)throw invalid();
            String type=identifier().toUpperCase(Locale.ROOT);
            if(!Set.of("BOOLEAN","TINYINT","SMALLINT","INT","INTEGER","BIGINT","LARGEINT","FLOAT","DOUBLE","DECIMAL","DECIMALV2","DECIMALV3","DECIMAL32","DECIMAL64","DECIMAL128","DECIMAL256","DATE","DATEV2","DATETIME","DATETIMEV2","CHAR","VARCHAR","STRING","JSON","JSONB","VARIANT","HLL","BITMAP","QUANTILE_STATE","ARRAY","MAP").contains(type))throw invalid();
            if(take("(")){integer();if(take(","))integer();require(")");}
            if(type.equals("ARRAY")){require("<");type();if(take("NOT"))require("NULL");require(">");}
            if(type.equals("MAP")){require("<");type();require(",");type();require(">");}
            typeDepth--;
        }
        void literal(){
            if(index<tokens.size()&&tokens.get(index).kind()==Kind.STRING){string();return;}
            if(take("NULL")||take("TRUE")||take("FALSE")||take("MAXVALUE")||take("MINVALUE")||take("?"))return;
            if(!take("-"))take("+");number();
        }
        void values(){require("(");literal();while(take(","))literal();require(")");}
        void partitionBy(boolean auto){
            require("PARTITION");require("BY");
            boolean range=take("RANGE");if(!range)require("LIST");require("(");
            if(range&&take("DATE_TRUNC")){require("(");identifier();require(",");if(!Set.of("hour","day","week","month","quarter","year").contains(string().toLowerCase(Locale.ROOT)))throw invalid();require(")");}
            else {if(auto&&range)throw invalid();identifier();while(take(","))identifier();}
            require(")");require("(");if(!take(")")){partition();while(take(","))partition();require(")");}
        }
        void partition(){require("PARTITION");ifNotExists();identifier();partitionValues();if(take("PROPERTIES"))properties();}
        void partitionValues(){
            require("VALUES");
            if(take("LESS")){require("THAN");if(take("MAXVALUE"))return;values();}
            else if(take("IN")){require("(");if(at("(")){values();while(take(","))values();}else{literal();while(take(","))literal();}require(")");}
            else {require("[");values();require(",");values();require(")");}
        }
        void distribution(){require("DISTRIBUTED");require("BY");if(take("HASH"))identifiers();else require("RANDOM");if(take("BUCKETS")){if(!take("AUTO"))integer();}}
        void properties(){require("(");string();require("=");string();while(take(",")){string();require("=");string();}require(")");}
        void indexDefinition(){require("INDEX");identifier();identifiers();require("USING");if(!take("BITMAP")&&!take("INVERTED")&&!take("NGRAM_BF"))throw invalid();if(take("PROPERTIES"))properties();if(take("COMMENT"))string();}
        void alter(){require("TABLE");table();action();while(take(","))action();}
        void action(){
            if(take("ADD")){
                boolean temporary=take("TEMPORARY");
                if(at("PARTITION")){partition();if(at("DISTRIBUTED"))distribution();if(take("PROPERTIES"))properties();return;}
                if(temporary)throw invalid();
                if(at("INDEX")){indexDefinition();return;}
                if(take("COLUMNS")){require("(");column();while(take(","))column();require(")");}
                else {take("COLUMN");column();position();}
            }else if(take("DROP")){
                boolean temporary=take("TEMPORARY");
                if(take("PARTITION")){ifExists();identifier();take("FORCE");}
                else {if(temporary)throw invalid();if(!take("INDEX"))take("COLUMN");identifier();}
            }else if(take("MODIFY")){
                if(take("PARTITION")){require("(");if(!take("*")){identifier();while(take(","))identifier();}require(")");require("SET");properties();}
                else {take("COLUMN");column();position();}
            }else if(take("RENAME")){
                if(take("COLUMN")||take("PARTITION")){identifier();require("TO");identifier();}
                else {take("TO");table();}
            }else if(take("REPLACE")){
                require("PARTITION");identifiers();require("WITH");require("TEMPORARY");require("PARTITION");identifiers();if(take("PROPERTIES"))properties();
            }else if(take("SET"))properties();
            else if(take("ORDER")){require("BY");identifiers();}
            else if(take("COMMENT"))string();
            else throw invalid();
        }
        void position(){if(take("FIRST"))return;if(take("AFTER"))identifier();}
        void drop(){if(!take("TABLE")&&!take("VIEW"))throw invalid();ifExists();table();while(take(","))table();take("FORCE");}
    }
}
