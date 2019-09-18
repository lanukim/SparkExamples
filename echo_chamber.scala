// Execution command for spark-shell in Scala
// set 50g memory
//     2g maxResultSize for heavy data set
// spark-shell --driver-memory 50g --conf spark.driver.maxResultSize=2g

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("example").getOrCreate()
import spark.implicits._

// white-list of citations
var citations = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/citations_whitelist/*.csv")
var papers = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/papers_journal_eng_article_1988_unique/*.csv")
var subjects_traditional = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/subjects_traditional.txt")
var wos_categorization = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("/home/lanu/GS/wos_categorization.csv")
var selfcited = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/selfcited_firstauthor/*.csv")

citations.createGlobalTempView("citations")
papers.createGlobalTempView("papers")
subjects_traditional.createGlobalTempView("subjects_traditional")
selfcited.createGlobalTempView("selfcited")

var subjects = Map(
 ("Communication", "Communication"),
 ("Economics", "Economics"),
 ("Public, Environmental & Occupational Health", "PublicHealth"),
 ("Education & Educational Research", "Education"),
 ("Law", "Law"),
 ("Business", "Business")
);

for((subject, subjectDirectory) <- subjects) {

    var subjectDirectorySuffix = "Dissertation/" + subjectDirectory
    var fields = """
        citations.UID AS citing_UID,
        (
            SELECT MAX(name)
            FROM global_temp.subjects_traditional as subjects
            WHERE subjects.UID = citations.UID AND subjects.no = 1
        ) AS citing_subject1,
        (
            SELECT MAX(title_source)
            FROM global_temp.papers as papers
            WHERE papers.UID = citations.UID
        ) AS citing_journal,
        citations.UID_year AS citing_year,

        citations.cited_UID,
        (
            SELECT MAX(name)
            FROM global_temp.subjects_traditional as subjects
            WHERE subjects.UID = citations.cited_UID AND subjects.no = 1
        ) AS cited_subject1,
        (
            SELECT MAX(title_source)
            FROM global_temp.papers as papers
            WHERE papers.UID = citations.cited_UID
        ) AS cited_journal,
        citations.cited_UID_year AS cited_year
    """

    var citers = spark.sql("""
    SELECT """
    + fields +
    """FROM global_temp.citations as citations
    WHERE
        EXISTS (
            SELECT papers.UID
            FROM global_temp.papers AS papers
                INNER JOIN global_temp.subjects_traditional AS subjects
                    ON papers.UID = subjects.UID
                    AND subjects.no <= 2
                    AND subjects.name = '""" + subject + """'
            WHERE citations.UID = papers.UID
            )
    """)

    citers.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/citers")

    var citees = spark.sql("""
    SELECT """
    + fields +
    """FROM global_temp.citations as citations
    WHERE
        EXISTS (
                SELECT papers.UID
                FROM global_temp.papers AS papers
                INNER JOIN global_temp.subjects_traditional AS subjects
                    ON papers.UID = subjects.UID
                    AND subjects.no <= 2
                    AND subjects.name = '""" + subject + """'
                WHERE citations.cited_UID = papers.UID
            )
    """)

    citees.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/citees")

    var papersInSubject = spark.sql("""
    SELECT
        papers.UID,
        papers.title_source,
        papers.pub_date,
        papers.page_count,
        papers.citation_count
    FROM global_temp.papers AS papers
    WHERE
        EXISTS (
            SELECT subjects.UID
            FROM global_temp.subjects_traditional AS subjects
            WHERE papers.UID = subjects.UID
                AND subjects.no <= 2
                AND subjects.name = '""" + subject + """'
            )
    """)

    papersInSubject.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/papersInSubject")

    papersInSubject = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/papersInSubject")
    spark.catalog.dropGlobalTempView("papersInSubject")
    papersInSubject.createGlobalTempView("papersInSubject")

    var subjectsInSubject = spark.sql("""
        SELECT subjects.UID, subjects.name
        FROM
            global_temp.papersInSubject
            INNER JOIN global_temp.subjects_traditional AS subjects
            ON papersInSubject.UID = subjects.UID
    """)

    subjectsInSubject.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/subjects")

    var selfcited = spark.sql("""
    SELECT
        selfcited.UID,
        selfcited.cited_UID,
        selfcited.cited_UID_year
    FROM global_temp.selfcited as selfcited
    WHERE
        EXISTS (
            SELECT papers.UID
            FROM global_temp.papers AS papers
            INNER JOIN global_temp.subjects_traditional AS subjects
                ON papers.UID = subjects.UID
                AND subjects.no <= 2
                AND subjects.name = '""" + subject + """'
            WHERE selfcited.cited_UID = papers.UID
            )
    """)

    selfcited.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/selfcited")
}

