#!/usr/bin/perl
use utf8;
use strict;
use DBI;
$|=1;

my $AUTO_HOME = $ENV{"AUTO_HOME"};
unshift(@INC, "${AUTO_HOME}/bin");
require etl_unix;
my $AUTO_DATA = "${AUTO_HOME}/DATA";
my $AUTO_LOG = "${AUTO_HOME}/LOG";

my $LANDDB = $ETL::LANDDB;
my $SDATADB = $ETL::SDATADB;
my $ODATADB = $ETL::ODATADB;
my $PDATADB = $ETL::PDATADB;
my $SUMDATADB = $ETL::SUMDATADB;
my $TMPDATADB = $ETL::TMPDATADB;
my $WORKDB = $ETL::WORKDB;
my $REPGRPBIDB = $ETL::REPGRPBIDB;
my $REPGRPBIWORKDB = $ETL::REPGRPBIWORKDB;
my $REPSFBIDB = $ETL::REPSFBIDB;
my $REPSFBIWORKDB = $ETL::REPSFBIWORKDB;
my $REPMDMDB = $ETL::REPMDMDB;
my $REPMDMWORKDB = $ETL::REPMDMWORKDB;
my $REPANALYDB = $ETL::REPANALYDB;

my $MINDATE = $ETL::MINDATE;
my $MAXDATE = $ETL::MAXDATE;
my $NULLDATE = $ETL::NULLDATE;
my $ILLDATE = $ETL::ILLDATE;
my $NULLDATE_TIME = $ETL::NULLDATE_TIME;
my $ILLDATE_TIME = $ETL::ILLDATE_TIME;

#beeline参数：the maximum width of the terminal
my $BEELINE_MAXWIDTH = $ETL::BEELINE_MAXWIDTH;
#获取步骤当前时间
my $GET_CURRENT_TIME = $ETL::GET_CURRENT_TIME;
#获取步骤num
my $GET_STEP_NUM = $ETL::GET_STEP_NUM;
#获取影响的记录数
my $GET_EFFECT_ROWS = $ETL::GET_EFFECT_ROWS;
#往控制台打印程序开始日志
my $BEGIN_LOG_PRINT = $ETL::BEGIN_LOG_PRINT;
#往日志表写入程序开始日志
my $BEGIN_LOG_WRITE = $ETL::BEGIN_LOG_WRITE;
#往控制台打印程序结束日志
my $END_LOG_PRINT = $ETL::END_LOG_PRINT;
#往日志表写入程序结束日志
my $END_LOG_WRITE = $ETL::END_LOG_WRITE;
#往控制台打印程序步骤日志
my $DETAIL_LOG_PRINT = $ETL::DETAIL_LOG_PRINT;
#往日志表写入程序步骤日志
my $DETAIL_LOG_WRITE = $ETL::DETAIL_LOG_WRITE;
#往控制台打印程序异常日志
my $EXCEPTION_LOG_PRINT = $ETL::EXCEPTION_LOG_PRINT;
#往日志表写入程序异常日志
my $EXCEPTION_LOG_WRITE = $ETL::EXCEPTION_LOG_WRITE;

#BEELINE登录文件
my $LOGON_FILE = "${AUTO_HOME}/etc/LOGON_DC";
my $CONTROL_FILE = "";
my $TX_DATE = "";
#作业算法
my ${ETL_POLICY}="F2";
#作业名称
my ${JOB_NAME} = "T09_FIN_ITEM_S01";
#脚本名称
my $SCRIPT = "t09_fin_item_s01200.pl";
#脚本用途
my $SCRIPT_USE = "Periodically Load data to ${JOB_NAME}";
#临时表名称
my ${TEMP_CUR_TABLE} = "${WORKDB}.T09_FIN_ITEM_S01_CUR_I";
#目标表名称
my ${TARGET_TABLE_NAME} = "${PDATADB}.t09_fin_item";
#目标表库名
my ${TARGET_DB_NAME} = "${PDATADB}";
#目标实体名称
my ${TARGET_ENTITY_NAME} = "t09_fin_item";
#主键字段
my ${TABLE_PK}="fin_item_id";

my ${ETL_POLICY}="F2";
my ${STABLE_SUFFIX_INC} = "";
my ${STABLE_SUFFIX_FULL} = "";
my ${Cache_SQL_File}="";
my ${Cache_LOG_File}="";

my $SERVER_HOST = $ETL::SERVER_HOST;
my $SERVER_PORT = $ETL::SERVER_PORT;
my $LOGON_STR;

#打印&写入程序开始日志
my $BEGIN_LOG = $BEGIN_LOG_PRINT.$BEGIN_LOG_WRITE.$GET_CURRENT_TIME;
#打印&写入程序步骤日志
my $DETAIL_LOG = $GET_EFFECT_ROWS.$GET_STEP_NUM.$DETAIL_LOG_PRINT.$DETAIL_LOG_WRITE.$GET_CURRENT_TIME;
#打印&写入程序结束日志
my $END_LOG = $END_LOG_PRINT.$END_LOG_WRITE;
#打印&写入程序异常日志
my $EXCEPTION_LOG = $GET_EFFECT_ROWS.$GET_STEP_NUM.$EXCEPTION_LOG_PRINT.$EXCEPTION_LOG_WRITE;

sub run_beeline_cmd()
{
   my $rc;
   my $strFileContent;
   my $KeyFinder;
   my $ShellCommandLine;
   my $strDoneSignal="{f1f09f87-8081-401a-b9b1-8d8384060753}_${TX_DATE}";
   $ShellCommandLine="beeline -u jdbc:hive2://$SERVER_HOST:$SERVER_PORT $LOGON_STR";
    
   ${Cache_SQL_File}="${AUTO_HOME}/TMP/${PDATADB}/$0_${TX_DATE}.hql";
   ${Cache_LOG_File}="${AUTO_HOME}/LOG/${PDATADB}/$0_${TX_DATE}.log";
    
   $rc=Prepare_SQL_Script();
   if ($rc!=0) {
      print "Failed to make SQL file.\n";
      return 1;
    }
   $strFileContent=ReadFile2String(${Cache_SQL_File});
   if (index($strFileContent,'--/*<!Revolution Script End!>*/')<0) {
      print "Incompleted SQL file [${Cache_SQL_File}].\n";
      return 1;
      }
   print $strFileContent;
   unlink (${Cache_LOG_File});
   my $wf;
   open($wf, ,">${Cache_LOG_File}");
   print $wf "$strFileContent \n";
   $strFileContent="";   
   close($wf);   

   print "Start to execute beeline command.\n";
   my $strShellCommand='';
   $strShellCommand="${ShellCommandLine} --verbose=true --showWarnings=true --showNestedErrs=true --hivevar IAMOK=$strDoneSignal -f '${Cache_SQL_File}' > '${Cache_LOG_File}' 2>&1";

   $rc = system($strShellCommand);
   $strFileContent=ReadFile2String(${Cache_LOG_File});
   print "\n$strFileContent\n";
   print "\nFinish executing beeline command.\n";

   $KeyFinder=index($strFileContent,$strDoneSignal);
   if ($KeyFinder>=0) {
       unlink (${Cache_SQL_File});
       unlink (${Cache_LOG_File});
       print "\nETL Job has been done successfully!\n";
       return 0;
      }
   else {
       print "\nFailed to execute ETL Job!\n";
       return 1;
      }
}

sub Prepare_SQL_Script
{
   my @ArgPara= @_;
   my $rc;
   my $fh;
   my $strSwitchOn;
   my $strSwitchOff;
   my $strTuningSetting;
   my $strSetSignal=' SET IAMOK;';

   unlink (${Cache_SQL_File}); #删除缓存SQL文件,此处起到防重跑的作用
   $rc= open($fh, '>:encoding(utf-8)', ${Cache_SQL_File}) ;
   unless ($rc) {
      print "Could not create cache file [${Cache_SQL_File}]\n";
      return 1;
    }
    
# ------ Below are transformation SQL----------
  print $fh <<ENDOFINPUT; 
--/*<!Revolution Script start!>*/

!set plsqlUseSlash true
set plsql.compile.dml.check.semantic=false;
set plsql.catch.hive.exception=true;
set transaction.type=inceptor;
set character.literal.as.string=true;

/* ======================== 变量声明 ============================= */
DECLARE
/* ======================== 日志变量 ============================= */
  v_step_num INT DEFAULT 0;                                    --程序步骤编号
  v_step_desc STRING DEFAULT '';                               --程序步骤描述
  v_step_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();           --程序步骤开始时间
  v_proc_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();           --程序总开始时间
  v_effect_rows INT DEFAULT 0;                                 --程序步骤影响的记录数
  v_tx_date STRING DEFAULT '${TX_DATE}';                       --ETL日期
  v_script_name STRING DEFAULT '$SCRIPT';                      --脚本名称
  v_script_use STRING DEFAULT '$SCRIPT_USE';                   --脚本用途

/* ======================== 异常变量 ============================= */
  v_error_message STRING DEFAULT '';                           --错误信息
  v_error_code INT DEFAULT 0;                                  --错误代码

/* ======================== 程序变量 ============================= */
  --v_busi_date varchar2(10) default replace(v_tx_date,'-','');

/* ======================== 程序主体 ============================= */
BEGIN
/* ====================== 开始日志模块 =========================== */
$BEGIN_LOG



/*======================================创建临时表加载当前数据======================================*/
v_step_desc := "创建临时表加载当前数据";


/* ======================= 临时表模块 ============================ */
v_step_desc := "临时表若不存在则创建，若已存在则清空数据";
EXECUTE IMMEDIATE "CREATE TABLE IF NOT EXISTS ${WORKDB}.T09_FIN_ITEM_S01_CUR_I_${TX_DATE} STORED AS ORC
     AS SELECT * FROM ${PDATADB}.T09_FIN_ITEM WHERE 1 = 2";
EXECUTE IMMEDIATE "TRUNCATE TABLE ${WORKDB}.T09_FIN_ITEM_S01_CUR_I_${TX_DATE}";

     
/* ==================== 组1:分布处理数据模块 ========================= */
v_step_desc := "组1-往临时表 加载数据";
INSERT INTO ${WORKDB}.T09_FIN_ITEM_S01_CUR_I_${TX_DATE}(
            fin_item_id                                                  
           ,item_nm                                                      
           ,leaf_item_ind                                                
           ,item_stat_cd                                                 
           ,mdm_sys_item_id                                                                                            
            )
SELECT
            COALESCE(A1.LABEL,'')                                        
           ,COALESCE(A2.DESCRIPTION,'')                                  
           ,CASE WHEN A3.itemid IS NOT NULL THEN '1' ELSE '0' END        
           ,'0'                                                          
           ,''                                                                                                          
FROM        ${SDATADB}.S01_HNICHFM_ACCOUNT_ITEM_${TX_DATE}  A1
LEFT JOIN   ${SDATADB}.S01_HNICHFM_ACCOUNT_DESC_${TX_DATE}  A2
       ON   A1.itemid = A2.itemid
AND A2.languageid = 0
LEFT JOIN   (
             SELECT laccount AS itemid
             FROM ${SDATADB}.S01_HNICHFM_DCE_1_2019_${TX_DATE}
             GROUP BY 1
             )  A3
       ON   A1.itemid = A3.itemid
;

/* ==================== 分布处理日志模块 ========================= */
$DETAIL_LOG

/* ====================== 结束日志模块 =========================== */
$END_LOG

/* ==================== 程序异常处理模块 ========================= */
EXCEPTION
  WHEN OTHERS THEN
    v_error_message := sqlerrm();
    v_error_code := sqlcode();
    $EXCEPTION_LOG
    ROLLBACK;
END;
/

$strSetSignal
--/*<!Revolution Script End!>*/
ENDOFINPUT
      close($fh);
      return 0;
}

sub CheckFileExist
{
       my $infile= shift;
       my $res=-1;     
       $res= (-e $infile);
       if ($res==1) {
                         return 1;
                     }
       else {
                         return 0;
                }
    
}

sub ReadFile2String
{
       my $infile="";
       $infile=shift;       
       if (CheckFileExist($infile)==0){
          return "";
          }       
       local $/=undef;
       open (FILE, $infile);
       my $strContent = <FILE>;
       close FILE;
       return $strContent;
}

sub trim($)
{
  my $string = shift;
  $string =~ s/^\s+//;
  $string =~ s/\s+$//;
  return $string;
}

# -------------- main function --------------

#------------------Funcrion MakeSurePackage------------------------
#if you use script template, you can call this function anywhere you wanted!
sub MakeSurePackage
{
   return 1;
}
#------------------End function MakeSurePackage--------------------

sub main()
{
  my $ret;
  open(LOGONFILE_H, "{LOGON_FILE}");
  $LOGON_STR = <LOGONFILE_H>;
  close(LOGONFILE_H);
  
  #Get the decode logon string
  #$LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
  $LOGON_STR = "-n etl -p etl";
  
  #Call beeline command to transform data
  $ret = run_beeline_cmd();
  print "run_beeline_cmd() = $ret\n";
  return $ret;
  
}

# ------------ program section ------------
#To see if there is one parameter,
#if there is no parameter,exit program
if ( $#ARGV < 0 ) {
  print "NO parameter enter!\n";
  exit(1);
}

$CONTROL_FILE = $ARGV[0];

$TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE}) - 8, 8);
if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE}) - 3, 3) eq 'dir' )
{
  $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE}) - 12, 8);
};
${STABLE_SUFFIX_INC} = substr($TX_DATE,length($TX_DATE) - 4, 4);

open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);




















# <--------------!Script End!---------->
