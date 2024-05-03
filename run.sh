#!/bin/bash
# 프로젝트 디렉토리로 이동
cd /Users/dabinkim/workspace/task/ActiveLogToHive/app

# 스파크 애플리케이션 빌드
mvn clean package



# 스파크 애플리케이션의 메인 클래스
MAIN_CLASS="com.task.App"
# 스파크 애플리케이션의 JAR 파일 경로
JAR_FILE="target/app-1.0-SNAPSHOT.jar"

# 스파크 실행 환경 변수 설정
SPARK_HOME="/Users/dabinkim/spark"
start_date="2019-11-10"
end_date="2019-11-11"

# 스파크 실행 명령어
$SPARK_HOME/bin/spark-submit \
--class $MAIN_CLASS \
$JAR_FILE \
$start_date $end_date