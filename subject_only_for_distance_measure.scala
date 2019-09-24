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
var papers = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/papers_journal_eng_article_1988_unique/*.csv")
var subjects_traditional = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/subjects_traditional.txt")

papers.createGlobalTempView("papers")
subjects_traditional.createGlobalTempView("subjects_traditional")

var subjects = Map(
 ("Biology", "Biology"),
 ("Business, Finance", "BusinessFinance"),
 ("Computer Science, Information Systems", "ComputerScienceInformationSystems"),
 ("Computer Science, Theory & Methods", "ComputerScienceTheoryAndMethods"),
 ("Demography", "Demography"),
 ("Education, Scientific Disciplines", "EducationScientificDisciplines"),
 ("Engineering, Industrial", "EngineerIndustrial"),
 ("Environmental Sciences", "EnvironmentalSciences"),
 ("Environmental Studies", "EnvironmentalStudies"),
 ("Family Studies", "FamilyStudies"),
 ("Health Care Sciences & Services", "HealthCareSciencesAndServices"),
 ("International Relations", "InternationalRelations"),
 ("Management", "Management"),
 ("Mathematical & Computational Biology", "MathematicalAndComputationalBiology"),
 ("Mathematics, Applied", "MathematicsApplied"),
 ("Medicine, General & Internal", "MedicineGeneralAndInternal"),
 ("Multidisciplinary Sciences", "MultidisciplinarySciences"),
 ("Oncology", "Oncology"),
 ("Physics, Mathematical", "PhysicsMathematical"),
 ("Physics, Multidisciplinary", "PhysicsMultidisciplinary"),
 ("Psychology, Applied", "PsychologyApplied"),
 ("Psychology, Clinical", "PsychologyClinical"),
 ("Psychology, Developmental", "PsychologyDevelopmental"),
 ("Psychology, Educational", "PsychologyEducational"),
 ("Psychology, Multidisciplinary", "PsychologyMultidisciplinary"),
 ("Psychology, Social", "PsychologySocial"),
 ("Public Administration", "PublicAdministration")
);

for((subject, subjectDirectory) <- subjects) {

    var subjectDirectorySuffix = "Distance/" + subjectDirectory
    var papersInSubject = spark.sql("""
    SELECT
        papers.UID,
        papers.title_source,
        papers.pub_date,
    FROM global_temp.papers AS papers
    WHERE
        EXISTS (
            SELECT subjects.UID
            FROM global_temp.subjects_traditional AS subjects
            WHERE papers.UID = subjects.UID
                AND subjects.no = 1
                AND subjects.name = '""" + subject + """'
            )
    """)

    papersInSubject.coalesce(1).write.format("csv").option("header","true").option("delimiter", "\t").save("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/papersInSubject")

    papersInSubject = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("mode", "DROPMALFORMED").load("/home/lanu/GS/data/echo_chamber/" + subjectDirectorySuffix + "/papersInSubject")
    }

