package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.springframework.stereotype.Component;

@Component
public class SqlGuard {
    public void validate(String sql,String database) {
        if(sql==null||sql.isBlank()||sql.length()>200_000) fail("请输入不超过 200000 字符的查询");
        if(!SqlParameters.extract(sql).isEmpty()) fail("SQL 参数尚未编译");
        String tokens=codeOnly(sql).toUpperCase(Locale.ROOT);
        if(tokens.matches("(?s).*\\b(INTO|UPDATE|DELETE|INSERT|REPLACE|LOCK|SHARE|OUTFILE|DUMPFILE|PROCEDURE|CALL|SET|LOAD_FILE|GET_LOCK|RELEASE_LOCK|RELEASE_ALL_LOCKS)\\b.*")||tokens.contains("@")||tokens.contains(":=")) fail("仅允许只读 SELECT / WITH 查询，禁止写入、锁定、文件及会话操作");
        try {
            var statements=CCJSqlParserUtil.parseStatements(sql,p -> p.withTimeOut(2000).withAllowComplexParsing(true).withBackslashEscapeCharacter(true));
            if(statements.size()!=1||!(statements.get(0) instanceof Select)) fail("仅允许一条 SELECT / WITH 查询");
            var finder=new TablesNamesFinder<Void>() {
                @Override public <S> Void visit(Function function,S context) {
                    String name=function.getName().replace("`","").replace("\"","").toUpperCase(Locale.ROOT);
                    if(function.getMultipartName().size()>1||Set.of("LOAD_FILE","GET_LOCK","RELEASE_LOCK","RELEASE_ALL_LOCKS","LAST_INSERT_ID").contains(name)) fail("不支持文件、会话或存储函数调用");
                    return super.visit(function,context);
                }
            };
            for(String table:finder.getTables(statements.get(0))) {
                String[] parts=table.replace("`","").replace("\"","").split("\\.");
                if(parts.length>2||(parts.length==2&&!parts[0].equalsIgnoreCase(database))) fail("只能查询当前数据源配置的业务库");
            }
        } catch(StudioException e) {throw e;} catch(Exception e) {fail("SQL 无法解析，或包含本期不支持的语法");}
    }
    // SQL-aware lexical pass complements AST parsing. Literals and ordinary comments
    // cannot masquerade as keywords; executable comments and optimizer hints are denied.
    private String codeOnly(String sql) {
        StringBuilder out=new StringBuilder();
        for(int i=0;i<sql.length();) {
            char ch=sql.charAt(i);
            if(ch=='\''||ch=='"'||ch=='`') {
                char quote=ch;out.append(' ');i++;boolean closed=false;
                while(i<sql.length()) {char c=sql.charAt(i++);if(c=='\\'&&i<sql.length()) {i++;continue;}if(c==quote) {if(i<sql.length()&&sql.charAt(i)==quote) {i++;continue;}closed=true;break;}}
                if(!closed) fail("SQL 引号未闭合");out.append(' ');
            } else if(ch=='#'||(ch=='-'&&i+2<sql.length()&&sql.charAt(i+1)=='-'&&Character.isWhitespace(sql.charAt(i+2)))) {
                while(i<sql.length()&&sql.charAt(i)!='\n')i++;out.append(' ');
            } else if(ch=='/'&&i+1<sql.length()&&sql.charAt(i+1)=='*') {
                if(i+2<sql.length()&&(sql.charAt(i+2)=='!'||sql.charAt(i+2)=='+')) fail("不支持可执行注释或优化器提示");
                int end=sql.indexOf("*/",i+2);if(end<0)fail("SQL 注释未闭合");i=end+2;out.append(' ');
            } else {out.append(ch);i++;}
        }
        return out.toString();
    }
    private void fail(String message) {throw StudioException.bad("READ_ONLY_SQL_REQUIRED",message);}
}
