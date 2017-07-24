/*
 * ARX: Powerful Data Anonymization
 * Copyright 2012 - 2017 Fabian Prasser, Florian Kohlmayer and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.deidentifier.arx.examples;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.EDDifferentialPrivacy;
import org.deidentifier.arx.risk.*;
import static java.lang.System.exit;

/**
 * This class demonstrates how to use (e,d)-DP, reading data from Cassandra.
 * @author João Gonçalves - Master student in Informatics Engineering of University of Coimbra
 */

public class Example54 extends Example
{
    private static double[] epsilons = {0.2,0.4,0.6,0.8,1,3,5},deltas = {0.0001,0.001,0.01,0.1,1};
    private static double[] riskThresholds ={0.01d,0.02d,0.03d,0.04d,0.05d},outliers = {0d,1d};
    private static double[] tValues = {0.1d,0.15d,0.2d,0.25d,0.3d,0.35d,0.4d};
    private static String[] genLevels={"LOW","MEDIUM","HIGH"};
    private static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/inv";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static int populationSize=7696365,repetitions=35,query1=-1,query2=-1,query3=-1,query4=-1;
    private static double numUniques=-1,averageRisk=-1,prosecutorRisk=-1,journalistRisk=-1,marketerRisk=-1;
    private static double highestRisk=-1,nHighestRisk=-1;
    private static double journalistRecordsRisk=-1,journalistHighestRisk=-1,journalistSuccessRate=-1;
    private static double prosecutorRecordsRisk=-1,prosecutorHighestRisk=-1,prosecutorSuccessRate=-1;
    private static double marketerSuccessRate=-1;
    private static Connection connection=null;
    private static Properties properties=null;

    /**
     * Main entry point.
     * It calls the method that connects to the MySQL database and calls the anonymization process for all parameters' values.
     * @param args
     */

    public static void main(String[] args)
    {
        connectDB();
        Data data = createDataSource();
        /*for(int i=0;i<genLevels.length;i++)
        {
            for(int j=0;j<epsilons.length;j++)
            {
                for(int k=0;k<deltas.length;k++)
                {
                    for(int l=0;l<riskThresholds.length;l++)
                    {
                        for(int m=0;m<outliers.length;m++)
                        {
                            for(int n=0;n<tValues.length;n++)
                            {
                                for(int o=1;o<=repetitions;o++)
                                {
                                    System.out.println("Initializing...repetition="+o +",epsilon=" + epsilons[j] + ",delta=" + deltas[k] + ",threshold=" + riskThresholds[l]
                                            + ",outlier=" + outliers[m] + ",t-value=" + tValues[n] + ",generalization level=" + genLevels[i] +".");
                                    anonymizeVoters(data,populationSize,epsilons[j],deltas[k],riskThresholds[l],outliers[m],tValues[n],genLevels[i]);
                                }
                            }
                        }
                    }
                }
            }
        }*/
        anonymizeVoters(data,populationSize,0.2,0.01,0.03,1d,0.2,"LOW");
        try
        {
            connection.close();
        }catch (SQLException e)
        {
            System.out.println("Error closing MySQL database connection.");
        }
        System.out.println("End.");
    }


    /**
     * This method connects to the MySQL database.
     */
    public static void connectDB()
    {
        if (properties == null)
        {
            properties = new Properties();
            properties.setProperty("user", USERNAME);
            properties.setProperty("password", PASSWORD);
        }
        if (connection == null)
        {
            try
            {
                Class.forName(DATABASE_DRIVER);
                connection = DriverManager.getConnection(DATABASE_URL, properties);
            } catch (ClassNotFoundException | SQLException e)
            {
                System.out.println("Error connecting to the MySQL database.");
                exit(1);
            }
        }
    }


    /**
     * This method writes the execution result to a file.
     * @param epsilon
     *              the epsilon value used in differential privacy
     * @param delta
     *              the delta value used in differential privacy
     * @param threshold
     *              acceptable highest probability of re-identification for a single record
     * @param maxOutliers
     *              maximum number of allowed outliers
     * @param tValue
     *              t value used in t-closeness
     * @param time
     *              execution time
     * @param maxInfoLoss
     *              maximum information loss
     * @param minInfoLoss
     *              minimum information loss
     * @param genLevel
     *              generalization level used
     * @param uniqueRecords
     *              number of unique records obtained in the MySQL database
     * @param optimalGen
     *              optimal generalization of the quasi-identifier attributes
     * @param queryResult
     *              results of the 4 queries executed against the anonymized dataset
     */
    public static void writeToFile(double epsilon,double delta,double threshold,double maxOutliers,double tValue,double time,String maxInfoLoss,String minInfoLoss,String genLevel,int uniqueRecords,String optimalGen,String queryResult)
    {
        try
        {
            FileWriter fw = new FileWriter("results.csv",true);
            fw.write("\""+epsilon + "\",\"" + delta + "\",\"" + threshold + "\",\""+  maxOutliers + "\",\""+  tValue
                    + "\",\""+  maxInfoLoss+ "\",\"" +minInfoLoss + "\",\"" + time + "\",\""+ uniqueRecords + "\",\""
                    + genLevel + "\",\""+ numUniques + "\",\""+ averageRisk + "\",\"" + prosecutorRisk + "\",\""
                    + journalistRisk + "\",\""+ marketerRisk + "\",\"" + highestRisk + "\",\"" + nHighestRisk + "\",\""
                    + journalistRecordsRisk + "\",\""+ journalistHighestRisk + "\",\""+ journalistSuccessRate
                    + "\",\""+ prosecutorRecordsRisk + "\",\""+ prosecutorHighestRisk + "\",\""+ prosecutorSuccessRate
                    + "\",\""+ marketerSuccessRate+ "\",\""+ optimalGen + "\",\""+ queryResult +  "\"\n");
            fw.close();
        }
        catch (Exception e)
        {
            System.out.println("Error writing results to file.");
            exit(1);
        }
    }


    /**
     * This method gets the risk metrics.
     * @param data
     *              data for which metrics will be obtained
     * @param  populationModel
     *              population for which metrics will be obtained
     * @param threshold
     *              acceptable highest probability of re-identification for a single record
     */
    public static void getMetrics(DataHandle data,ARXPopulationModel populationModel,double threshold)
    {
        RiskEstimateBuilder riskEstimator = data.getRiskEstimator(populationModel);
        RiskModelSampleSummary riskModelSampleSummary = riskEstimator.getSampleBasedRiskSummary(threshold);
        //get risk estimator metrics
        numUniques = riskEstimator.getSampleBasedUniquenessRisk().getNumUniqueTuples();
        averageRisk = riskEstimator.getSampleBasedReidentificationRisk().getAverageRisk();
        journalistRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedJournalistRisk();
        marketerRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedMarketerRisk();
        prosecutorRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedProsecutorRisk();
        highestRisk = riskEstimator.getSampleBasedReidentificationRisk().getHighestRisk();
        nHighestRisk = riskEstimator.getSampleBasedReidentificationRisk().getNumTuplesAffectedByHighestRisk();
        //get risk model sample summary metrics
        journalistRecordsRisk=riskModelSampleSummary.getJournalistRisk().getRecordsAtRisk();
        journalistHighestRisk = riskModelSampleSummary.getJournalistRisk().getHighestRisk();
        journalistSuccessRate=riskModelSampleSummary.getJournalistRisk().getSuccessRate();
        prosecutorRecordsRisk=riskModelSampleSummary.getProsecutorRisk().getRecordsAtRisk();
        prosecutorHighestRisk=riskModelSampleSummary.getProsecutorRisk().getHighestRisk();
        prosecutorSuccessRate=riskModelSampleSummary.getProsecutorRisk().getSuccessRate();
        marketerSuccessRate=riskModelSampleSummary.getMarketerRisk().getSuccessRate();
    }

    /**
     * This method prints the risk metrics.
     * @param data
     *              data for which metrics will be obtained
     * @param  populationModel
     *              population for which metrics will be obtained
     */
    public static void printMetrics(DataHandle data,ARXPopulationModel populationModel)
    {
        //print quasi identifiers distinction and separation
        for(RiskModelAttributes.QuasiIdentifierRisk qi: data.getRiskEstimator(populationModel).getAttributeRisks().getAttributeRisks())
        {
            for(int i=0;i<qi.getIdentifier().size();i++)
            {
                if(i!= qi.getIdentifier().size()-1)
                    System.out.print(qi.getIdentifier().get(i) + ",");
                else
                    System.out.print(qi.getIdentifier().get(i) + "-->");
            }
            System.out.println(" distinction: " + qi.getDistinction() + ", separation: " + qi.getSeparation());
        }
        //print risk estimator metrics
        System.out.println("\n");
        System.out.println("Number of uniques: " + numUniques);
        System.out.println("Average risk: " + averageRisk);
        System.out.println("Journalist re-identification risk: " + journalistRisk);
        System.out.println("Marketer re-identification risk: " + marketerRisk);
        System.out.println("Prosecutor re-identification risk: " + prosecutorRisk);
        System.out.println("Highest Risk: " + highestRisk);
        System.out.println("Number of tuples affected by highest risk: " + nHighestRisk);
        //print risk model sample summary metrics
        System.out.println("Journalist attacker model-Records at risk: " + journalistRecordsRisk);
        System.out.println("Journalist attacker model-Highest risk: " + journalistHighestRisk);
        System.out.println("Journalist attacker model-Success rate: " + journalistSuccessRate);
        System.out.println("Prosecutor attacker model-Records at risk: " + prosecutorRecordsRisk);
        System.out.println("Prosecutor attacker model-Highest risk: " + prosecutorHighestRisk);
        System.out.println("Prosecutor attacker model-Success rate: " + prosecutorSuccessRate);
        System.out.println("Marketer attacker model-Success rate: " + marketerSuccessRate);
    }

    /**
     * This method gets the query results for the anonymized data.
     * @return string with results separated by commas
     */
    public static String getQueryResults()
    {
        Statement stmt;
        String query, result = "";
        query = "select count(*) from voters where city like '%NC%' and race_code='B' and gender_code='F' and party_cd='DEM';";
        ResultSet resultSet=null;
        try
        {
            stmt = (Statement) connection.createStatement();
            resultSet = stmt.executeQuery(query);
            while(resultSet.next())
            {
                query1 =  resultSet.getInt(1);
            }
        }catch (SQLException e)
        {
            System.out.println("Error getting query 1 result.");
        }
        query = "select count(*) from voters where ethnic_code='NL' and age='Age Over 66' and party_cd='REP';";
        try
        {
            stmt = (Statement) connection.createStatement();
            resultSet = stmt.executeQuery(query);
            while(resultSet.next())
            {
                query2 =  resultSet.getInt(1);
            }
        }catch (SQLException e)
        {
            System.out.println("Error getting query 2 result.");
        }
        query = "select count(*) from voters where voter_stat='ACTIVE|VERIFIED' and city like '%NC%' and party_cd='DEM';";
        try
        {
            stmt = (Statement) connection.createStatement();
            resultSet = stmt.executeQuery(query);
            while(resultSet.next())
            {
                query3 =  resultSet.getInt(1);
            }
        }catch (SQLException e)
        {
            System.out.println("Error getting query 3 result.");
        }
        query = "select count(*) from voters where voter_stat='REMOVED|MOVED FROM STATE' and city like '%NC%' and birth_place like '%NY%';";
        try
        {
            stmt = (Statement) connection.createStatement();
            resultSet = stmt.executeQuery(query);
            while(resultSet.next())
            {
                query4 =  resultSet.getInt(1);
            }
        }catch (SQLException e)
        {
            System.out.println("Error getting query 4 result.");
        }
        result += query1 + "\",\"" +query2 + "\",\"" +query3 + "\",\"" +query4;
        return result;
    }


    /**
     * This method gets the number of unique records in a dataset.
     * @param data
     *              data for which number of unique records will be obtained
     * @return number of unique records in a dataset
     */
    public static int getUniqueRecords(DataHandle data)
    {
        Statement stmt;
        String query;
        int uniqueRecords=-1;
        try (Statement statement = (Statement) connection.createStatement())
        {
            statement.executeUpdate("TRUNCATE voters_anon");
        } catch (SQLException e)
        {
            System.out.println("Error truncating voters_anon.");
        }
        File newFile = new File("current.csv");
        try
        {
            data.save(newFile,',');
        } catch (IOException e)
        {
            System.out.println("Error saving data to 'current.csv'.");
        }
        try
        {
            stmt = (Statement) connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            query = "load data local infile '/Users/joaogoncalves/Documents/myfork-arx/current.csv' into table voters_anon fields terminated by ',' ignore 1 lines";
            stmt.executeUpdate(query);
        }
        catch(Exception e)
        {
            System.out.println("Error loading 'current.csv' to the MySQL database.");
        }
        query = "select count(distinct voter_stat,city,zip_code,race_code,ethnic_code,party_cd,gender_code,birth_place,drivers_lic) from voters_anon;";
        try
        {
            stmt = (Statement) connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(query);
            while(resultSet.next())
            {
                uniqueRecords =  resultSet.getInt(1);
            }
        }catch (SQLException e)
        {
            System.out.println("Error counting number of unique records.");
        }
        //newFile.delete();
        return uniqueRecords;
    }

    /**
     * This method gets the optimal generalization for the quasi-identifier attributes(city,voter_stat,birth_place and zip_code).
     * @param result
     *              result of the anonymization
     * @param data
     *              data(and its configuration) for which optimal generalization will be obtained
     * @return      string with optimal generalization for each quasi-identifier attribute, separated by commas
     */
    public static String getOptimalGeneralization(final ARXResult result, final Data data)
    {
        String optimalGen = "";
        final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
        final List<String> qis = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());
        for (int i = 0; i < qis.size(); i++)
        {
            if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
            {
                optimalGen += (double) optimum.getGeneralization(qis.get(i))/(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
                if (i!=qis.size()-1)
                    optimalGen+= "\",\"";
            }
        }
        return optimalGen;
    }


    /**
     * This method creates the data source.
     * @return      data source
     */
    public static Data createDataSource()
    {
        System.out.println("Creating data source...");
        Data data=null;
        AttributeType.Hierarchy voterStatHierarchy= null,zipCodeHierarchy= null,cityHierarchy= null,birthStateHierarchy= null,raceCodeHierarchy=null,ethnicCodeHierarchy=null,partyCdHierarchy=null,genderCodeHierarchy=null,driversLicHierarchy=null;
        //establish cassandra connection
        DataSource source = DataSource.createCassandraSource("127.0.0.1","test","voters");
        //add columns
        //source.addColumn("id","id",DataType.INTEGER);
        source.addColumn("voter_stat","voter_stat",DataType.STRING);
        source.addColumn("city","city",DataType.STRING);
        source.addColumn("zip_code","zip_code",DataType.STRING);
        source.addColumn("race_code","race_code",DataType.STRING);
        source.addColumn("ethnic_code","ethnic_code",DataType.STRING);
        source.addColumn("party_cd","party_cd",DataType.STRING);
        source.addColumn("gender_code","gender_code",DataType.STRING);
        source.addColumn("birth_place","birth_place",DataType.STRING);
        source.addColumn("drivers_lic","drivers_lic",DataType.STRING);
        source.addColumn("age","age",DataType.STRING);
        //create data source and its population model
        try
        {
            data = Data.create(source);
        } catch (IOException e)
        {
            System.out.println("Error creating data source.");
            exit(1);
        }
        //build hierarchies for each quasi-identifier attribute
        try
        {
            voterStatHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/voter_stat_hierarchy.csv"), StandardCharsets.UTF_8,',');
            cityHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/cities_hierarchy.csv"), StandardCharsets.UTF_8,',');
            zipCodeHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/zipcode_hierarchy.csv"), StandardCharsets.UTF_8,',');
            raceCodeHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/race_code_hierarchy.csv"), StandardCharsets.UTF_8,',');
            ethnicCodeHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/ethnic_code_hierarchy.csv"), StandardCharsets.UTF_8,',');
            partyCdHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/party_cd_hierarchy.csv"), StandardCharsets.UTF_8,',');
            genderCodeHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/gender_code_hierarchy.csv"), StandardCharsets.UTF_8,',');
            birthStateHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/birth_state_hierarchy.csv"), StandardCharsets.UTF_8,',');
            driversLicHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/drivers_lic_hierarchy.csv"), StandardCharsets.UTF_8,',');
        } catch (IOException e)
        {
            System.out.println("Error creating hierarchies.");
            exit(1);
        }
        //set type of each attribute(insensitive, sensitive, quasi-identifier or identifier)
        //data.getDefinition().setAttributeType("id", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("voter_stat", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("city", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("zip_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("race_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("ethnic_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("party_cd", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("gender_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("birth_place", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("drivers_lic", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("age", AttributeType.INSENSITIVE_ATTRIBUTE);
        //set hierarchies for each quasi-identifier attribute
        data.getDefinition().setHierarchy("voter_stat", voterStatHierarchy);
        data.getDefinition().setHierarchy("zip_code", zipCodeHierarchy);
        data.getDefinition().setHierarchy("city", cityHierarchy);
        data.getDefinition().setHierarchy("race_code",raceCodeHierarchy);
        data.getDefinition().setHierarchy("ethnic_code",ethnicCodeHierarchy);
        data.getDefinition().setHierarchy("party_cd",partyCdHierarchy);
        data.getDefinition().setHierarchy("gender_code",genderCodeHierarchy);
        data.getDefinition().setHierarchy("birth_place",birthStateHierarchy);
        data.getDefinition().setHierarchy("drivers_lic",driversLicHierarchy);
        return data;
    }


    /**
     * Method responsible for construct the data source and anonymize it.
     * @param  populationSize
     *              size of the population that is in the dataset
     * @param epsilon
     *              the epsilon value used in differential privacy
     * @param delta
     *              the delta value used in differential privacy
     * @param threshold
     *              acceptable highest probability of re-identification for a single record
     * @param maxOutliers
     *              maximum number of allowed outliers
     * @param tValue
     *              t value used in t-closeness
     * @param genLevel
     *              generalization level used
     */
    public static void anonymizeVoters(Data data,int populationSize,double epsilon, double delta,double threshold,double maxOutliers,double tValue,String genLevel)
    {
        System.out.println("Getting ready...");
        ARXResult result = null;
        EDDifferentialPrivacy criterion;
        ARXPopulationModel populationModel = ARXPopulationModel.create(populationSize);
        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Create a differential privacy criterion (epsilon,delta)
        if(genLevel.equals("LOW"))
        {
            criterion = new EDDifferentialPrivacy(epsilon,delta, DataGeneralizationScheme.create(data, DataGeneralizationScheme.GeneralizationDegree.LOW));
        }
        else if(genLevel.equals("MEDIUM"))
        {
            criterion = new EDDifferentialPrivacy(epsilon,delta, DataGeneralizationScheme.create(data, DataGeneralizationScheme.GeneralizationDegree.MEDIUM));
        }
        else
        {
            criterion = new EDDifferentialPrivacy(epsilon,delta, DataGeneralizationScheme.create(data, DataGeneralizationScheme.GeneralizationDegree.HIGH));
        }
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(criterion);
        config.setMaxOutliers(maxOutliers);
        //get input metrics
        //getMetrics(data.getHandle(),populationModel,threshold);
        //anonymize input
        System.out.println("Starting anonymization...");
        try
        {
            result = anonymizer.anonymize(data, config);
        } catch (IOException e)
        {
            System.out.println("Error anonymizing data.");
            exit(1);
        }
        DataHandle optimal = result.getOutput();
        //get output metrics
        getMetrics(optimal,populationModel,threshold);
        //write execution result to file
        System.out.println("Writing execution result to file...");
        writeToFile(epsilon,delta,threshold,maxOutliers,tValue,result.getTime()/1000d,result.getGlobalOptimum().getMaximumInformationLoss().toString(),result.getGlobalOptimum().getMinimumInformationLoss().toString(),genLevel,getUniqueRecords(optimal),getOptimalGeneralization(result,data),getQueryResults());
        data.getHandle().release();
    }
}
