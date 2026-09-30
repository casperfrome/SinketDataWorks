package com.fake.dataworks.service;

import com.fake.dataworks.exception.StudioException;
import java.util.*;
import java.util.regex.Pattern;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.util.TablesNamesFinder;

/** A script is validated in full before any of its original statements reaches JDBC. */
public record SqlScript(List<Command> commands) {
    public record Command(SqlParameters parameters, String kind) {
        public boolean writes() { return !kind.equals("QUERY"); }
        @Override public String toString() { return "Command["+kind+"]"; }
    }
    public boolean writes() { return commands.stream().anyMatch(Command::writes); }
    private static final String IDENT="(?:`(?:[^`]|``)+`|[\\p{L}_][\\p{L}\\p{N}_$]*)";
    private static final String TABLE=IDENT+"(?:\\s*\\.\\s*"+IDENT+")?";
    private static final Set<String> FORBIDDEN=Set.of("OUTFILE","DUMPFILE","LOAD_FILE","GET_LOCK","RELEASE_LOCK","RELEASE_ALL_LOCKS","LAST_INSERT_ID","PROCEDURE","DEFINER","TABLESPACE","DIRECTORY","CONNECTION");

    public static SqlScript prepare(String input, String database, SqlGuard queryGuard) {
        return prepare(input,database,queryGuard,"MYSQL");
    }
    public static SqlScript prepare(String input, String database, SqlGuard queryGuard,String dialect) {
        if(!Set.of("MYSQL","DORIS").contains(dialect))fail("不支持的数据源 SQL 方言");
        var commands=new ArrayList<Command>();
        for(String sql:split(input)) {
            var bound=SqlParameters.compile(sql);
            commands.add(new Command(bound,"DORIS".equals(dialect)?DorisSqlValidator.validate(bound.sql(),database,queryGuard):validate(bound.sql(),database,queryGuard)));
        }
        return new SqlScript(List.copyOf(commands));
    }

    // Remove comments while splitting, so MySQL # comments and embedded semicolons are unambiguous.
    public static List<String> split(String input) {
        if(input==null||input.isBlank()||input.length()>200_000) fail("请输入不超过 200000 字符的 SQL");
        var statements=new ArrayList<String>();var current=new StringBuilder();
        for(int i=0;i<input.length();) {
            char c=input.charAt(i);
            if(c=='\''||c=='"'||c=='`') {
                char quote=c;current.append(c);i++;boolean closed=false;
                while(i<input.length()) {
                    c=input.charAt(i++);current.append(c);
                    if(c=='\\'&&i<input.length()) {current.append(input.charAt(i++));continue;}
                    if(c==quote) {if(i<input.length()&&input.charAt(i)==quote)current.append(input.charAt(i++));else {closed=true;break;}}
                }
                if(!closed) fail("SQL 引号未闭合");
            } else if(c=='#'||(c=='-'&&i+2<input.length()&&input.charAt(i+1)=='-'&&Character.isWhitespace(input.charAt(i+2)))) {
                while(i<input.length()&&input.charAt(i)!='\n')i++;current.append('\n');
            } else if(c=='/'&&i+1<input.length()&&input.charAt(i+1)=='*') {
                if(i+2<input.length()&&(input.charAt(i+2)=='!'||input.charAt(i+2)=='+')) fail("不支持可执行注释或优化器提示");
                int end=input.indexOf("*/",i+2);if(end<0)fail("SQL 注释未闭合");i=end+2;current.append(' ');
            } else if(c==';') {add(statements,current);i++;}
            else {current.append(c);i++;}
        }
        add(statements,current);
        if(statements.isEmpty())fail("请输入 SQL 语句");
        if(statements.size()>1000)fail("一次运行最多支持 1000 条 SQL");
        return statements;
    }
    private static void add(List<String> statements,StringBuilder text) {
        if(!text.toString().isBlank())statements.add(text.toString().strip());text.setLength(0);
    }

    static String validate(String sql,String database,SqlGuard queryGuard) {
        return validate(sql,database,queryGuard,false);
    }
    static String validate(String sql,String database,SqlGuard queryGuard,boolean doris) {
        // Literals are excluded, quoted identifiers included. This also covers DDL expressions
        // that TablesNamesFinder does not visit, such as generated-column definitions.
        String code=sql.replaceAll("'(?:(?:'')|(?:\\\\.)|[^'\\\\])*'|\"(?:(?:\"\")|(?:\\\\.)|[^\"\\\\])*\""," ").replace("`","");
        for(String word:FORBIDDEN)if(Pattern.compile("(?i)\\b"+word+"\\b").matcher(code).find())fail("不支持文件、会话、存储程序或外部存储操作");
        if(code.contains("@")||code.contains(":="))fail("不支持会话变量");
        // JSqlParser 5.3 does not represent these MySQL forms completely. Strict anchored
        // grammars cover all their object names, while JDBC receives the original SQL.
        var dropIndex=Pattern.compile("(?is)^DROP\\s+INDEX\\s+"+IDENT+"\\s+ON\\s+("+TABLE+")$").matcher(sql);
        if(dropIndex.matches()){checkName(dropIndex.group(1),database);return "DDL";}
        var rename=Pattern.compile("(?is)^ALTER\\s+TABLE\\s+("+TABLE+")\\s+RENAME\\s+(?:TO\\s+|AS\\s+)?("+TABLE+")$").matcher(sql);
        if(rename.matches()){checkName(rename.group(1),database);checkName(rename.group(2),database);return "DDL";}
        var drop=Pattern.compile("(?is)^DROP\\s+(?:TEMPORARY\\s+)?(?:TABLE|VIEW)\\s+(?:IF\\s+EXISTS\\s+)?("+TABLE+"(?:\\s*,\\s*"+TABLE+")*)(?:\\s+(?:RESTRICT|CASCADE))?$").matcher(sql);
        if(drop.matches()) {var names=Pattern.compile(TABLE).matcher(drop.group(1));while(names.find())checkName(names.group(),database);return "DDL";}
        try {
            Statement statement=CCJSqlParserUtil.parse(sql,p->p.withTimeOut(2000).withAllowComplexParsing(true).withBackslashEscapeCharacter(true));
            if(statement instanceof Select) {if(doris)queryGuard.validateDoris(sql,database);else queryGuard.validate(sql,database);return "QUERY";}
            boolean dml=statement instanceof Insert||statement instanceof Update||statement instanceof Delete||statement instanceof Upsert;
            boolean ddl=statement instanceof CreateTable||statement instanceof Alter||statement instanceof RenameTableStatement||statement instanceof Truncate||statement instanceof CreateIndex||statement instanceof CreateView||statement instanceof AlterView;
            if(!dml&&!ddl)fail("仅支持查询、增删改，以及表、索引和视图的结构操作");
            if(ddl) {
                // Inline column references and DDL expression specs are stored as token lists,
                // not visited by TablesNamesFinder. Include those object references explicitly.
                var references=Pattern.compile("(?i)\\bREFERENCES\\s+("+TABLE+")").matcher(code);
                while(references.find())checkName(references.group(1),database);
                var qualifiedCalls=Pattern.compile("("+IDENT+"\\s*\\.\\s*"+IDENT+")\\s*\\(").matcher(code);
                while(qualifiedCalls.find())checkName(qualifiedCalls.group(1),database);
            }
            var finder=new TablesNamesFinder<Void>() {
                @Override public <S> Void visit(TableFunction function,S context) {
                    if(doris)fail("不支持外部或系统表函数，只能操作当前数据源配置的业务库");
                    return super.visit(function,context);
                }
                @Override public <S> Void visit(Function function,S context) {
                    String name=function.getName().replace("`","").replace("\"","").toUpperCase(Locale.ROOT);
                    if(function.getMultipartName().size()>1||FORBIDDEN.contains(name))fail("不支持文件、会话或存储函数调用");
                    return super.visit(function,context);
                }
            };
            if(statement instanceof CreateIndex index) check(index.getTable(),database);
            else if(statement instanceof AlterView view) {check(view.getView(),database);queryGuard.validate(view.getSelect().toString(),database);}
            else for(String name:finder.getTables(statement))checkName(name,database);
            if(statement instanceof CreateTable table) {
                check(table.getLikeTable(),database);
                if(table.getIndexes()!=null)for(var index:table.getIndexes())if(index instanceof ForeignKeyIndex fk)check(fk.getTable(),database);
                if(table.getSelect()!=null)queryGuard.validate(table.getSelect().toString(),database);
            }
            if(statement instanceof CreateView view)queryGuard.validate(view.getSelect().toString(),database);
            if(statement instanceof Alter alter)for(var expression:alter.getAlterExpressions()) {
                if(expression.getNewTableName()!=null)checkName(expression.getNewTableName(),database);
                if(expression.getFkSourceSchema()!=null&&!unquote(expression.getFkSourceSchema()).equalsIgnoreCase(database))fail("只能操作当前数据源配置的业务库");
                if(expression.getFkSourceTable()!=null)checkName(expression.getFkSourceTable(),database);
                if(expression.getIndex() instanceof ForeignKeyIndex fk)check(fk.getTable(),database);
                if(expression.getExchangePartitionTableName()!=null)checkName(expression.getExchangePartitionTableName(),database);
            }
            return dml?"UPDATE":"DDL";
        } catch(StudioException e) {throw e;} catch(Exception e) {fail("SQL 无法解析，或包含不支持的语法");return "";}
    }
    private static void check(Table table,String database) {if(table!=null)checkName(table.getFullyQualifiedName(),database);}
    private static void checkName(String name,String database) {
        var match=Pattern.compile("^\\s*("+IDENT+")(?:\\s*\\.\\s*("+IDENT+"))?\\s*$").matcher(name);
        if(!match.matches()||(match.group(2)!=null&&!unquote(match.group(1)).equalsIgnoreCase(database)))fail("只能操作当前数据源配置的业务库");
    }
    private static String unquote(String value) {return value.startsWith("`")?value.substring(1,value.length()-1).replace("``","`"):value;}
    private static void fail(String message) {throw StudioException.bad("UNSUPPORTED_SQL",message);}
}
