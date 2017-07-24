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
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.AverageReidentificationRisk;
import org.deidentifier.arx.criteria.EDDifferentialPrivacy;
import org.deidentifier.arx.exceptions.RollbackRequiredException;
import org.deidentifier.arx.metric.Metric;
import org.deidentifier.arx.risk.RiskEstimateBuilder;
import static java.lang.System.exit;

/**
 * This class demonstrates how to use (e,d)-DP, reading data from Cassandra.
 * @author João Gonçalves - Master student in Informatics Engineering of University of Coimbra
 */

public class Example55 extends Example
{
    private static double[] epsilons = {0.2,0.4,0.6,0.8,1,3,5},deltas = {0.0001,0.001,0.01,0.1,1};
    private static String[] genLevels={"LOW","MEDIUM","HIGH"}, metrics={"Loss","Entropy","Height","Precision"};
    private static double supressionLimit=0.03d,averageReidentificationRiskThreshold=0.2d;
    private static int populationSize=7696365,repetitions=35;

    /**
     * Main entry point.
     * It calls the method that connects to the MySQL database and calls the anonymization process for all parameters' values.
     * @param args
     */

    public static void main(String[] args)
    {
        Data data = createDataSource();
        for(int i=0;i<metrics.length;i++)
        {
            for(int j=0;j<genLevels.length;j++)
            {
                for(int k=0;k<epsilons.length;k++)
                {
                    for(int l=0;l<deltas.length;l++)
                    {
                        for(int m=0;m<repetitions;m++)
                        {
                            System.out.println("repetition=" + m + ",metric=" + metrics[i]+ ",gen level=" +genLevels[j]+ ",epsilon=" +epsilons[k]+ ",delta=" +deltas[l]);
                            anonymizeVoters(data,metrics[i],genLevels[j],epsilons[k],deltas[l]);
                        }
                    }
                }
            }
        }
        System.out.println("End.");
    }


    /**
     * This method writes the execution result to a file.
     */
    public static void writeToFile(String toWrite)
    {
        try
        {
            FileWriter fw = new FileWriter("results.csv",true);
            fw.write(toWrite);
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
     */
    public static String getMetrics(DataHandle data,ARXPopulationModel populationModel)
    {
        RiskEstimateBuilder riskEstimator = data.getRiskEstimator(populationModel);
        //formatter
        DecimalFormat df = new DecimalFormat("#.#####");
        DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
        otherSymbols.setDecimalSeparator(',');
        df.setDecimalFormatSymbols(otherSymbols);
        //get risk estimator metrics
        double numUniques = riskEstimator.getSampleBasedUniquenessRisk().getNumUniqueTuples();
        double averageRisk = riskEstimator.getSampleBasedReidentificationRisk().getAverageRisk();
        double journalistRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedJournalistRisk();
        double marketerRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedMarketerRisk();
        double  prosecutorRisk = riskEstimator.getSampleBasedReidentificationRisk().getEstimatedProsecutorRisk();
        double highestRisk = riskEstimator.getSampleBasedReidentificationRisk().getHighestRisk();
        double nHighestRisk = riskEstimator.getSampleBasedReidentificationRisk().getNumTuplesAffectedByHighestRisk();
        return df.format(numUniques) + "\",\"" + df.format(averageRisk) + "\",\"" + df.format(journalistRisk) + "\",\"" + df.format(marketerRisk) + "\",\"" + df.format(prosecutorRisk) + "\",\"" + df.format(highestRisk) + "\",\"" + df.format(nHighestRisk);
    }

    /**
     * This method creates the data source.
     * @return      data source
     */
    public static Data createDataSource()
    {
        System.out.println("Creating data source...");
        Data data=null;
        AttributeType.Hierarchy voterStatHierarchy= null,zipCodeHierarchy= null,cityHierarchy= null,birthStateHierarchy= null,raceCodeHierarchy=null,
                ethnicCodeHierarchy=null,partyCdHierarchy=null,genderCodeHierarchy=null,driversLicHierarchy=null,ageHierarchy=null;
        //establish cassandra connection
        DataSource source = DataSource.createCassandraSource("127.0.0.1","test","voters");
        //add columns
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
            ageHierarchy = AttributeType.Hierarchy.create(new File("src/voters_hierarchies/age_hierarchy.csv"), StandardCharsets.UTF_8,',');
        } catch (IOException e)
        {
            System.out.println("Error creating hierarchies.");
            exit(1);
        }
        //set type of each attribute(insensitive, sensitive, quasi-identifier or identifier)
        data.getDefinition().setAttributeType("voter_stat", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("city", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("zip_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("race_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("ethnic_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("party_cd", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("gender_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("birth_place", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("drivers_lic", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("age", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
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
        data.getDefinition().setHierarchy("age",ageHierarchy);
        return data;
    }


    /**
     * Method responsible for construct the data source and anonymize it.
     * @param epsilon
     *              the epsilon value used in differential privacy
     * @param delta
     *              the delta value used in differential privacy
     */
    public static void anonymizeVoters(Data data,String metric,String genLevel,double epsilon, double delta)
    {
        System.out.println("Getting ready...");
        ARXResult result = null;
        EDDifferentialPrivacy criterion;
        ARXPopulationModel populationModel = ARXPopulationModel.create(populationSize);
        String riskMetrics=null;
        //formatter
        DecimalFormat df = new DecimalFormat("#.####");
        DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
        otherSymbols.setDecimalSeparator(',');
        df.setDecimalFormatSymbols(otherSymbols);
        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Create a differential privacy criterion (epsilon,delta) with LOW, MEDIUM or HIGH generalization level
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
        //set supression limit
        config.setMaxOutliers(supressionLimit);
        //add AverageReidentificationRisk criterion
        config.addPrivacyModel(new AverageReidentificationRisk(averageReidentificationRiskThreshold));
        //select metric
        if(metric.equals("Loss"))
        {
            config.setQualityModel(Metric.createLossMetric());
        }
        else if(metric.equals("Entropy"))
        {
            config.setQualityModel(Metric.createEntropyMetric(false,0.5, Metric.AggregateFunction.GEOMETRIC_MEAN));
        }
        else if(metric.equals("Height"))
        {
            config.setQualityModel(Metric.createHeightMetric(Metric.AggregateFunction.GEOMETRIC_MEAN));
        }
        else
        {
            config.setQualityModel(Metric.createPrecisionMetric(Metric.AggregateFunction.GEOMETRIC_MEAN));
        }
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
        //optimize output data utility
        try
        {
            result.optimize(result.getOutput());
        } catch (RollbackRequiredException e)
        {
            System.out.println("Error optimizing data utility.");
            exit(1);
        }
        DataHandle optimal = result.getOutput();
        riskMetrics = getMetrics(optimal,populationModel);
        //write execution result to file
        System.out.println("Writing... execution result to file");
        String aux = "\""+ metric + "\",\"" + genLevel + "\",\"" + df.format(epsilon) + "\",\"" + df.format(delta)
                + "\",\""+ df.format(Double.valueOf(result.getGlobalOptimum().getHighestScore().toString())) + "\",\""
                + df.format(Double.valueOf(result.getGlobalOptimum().getLowestScore().toString())) + "\",\"" + df.format(result.getTime()/1000d)
                + "\",\"" + riskMetrics +  "\"\n";
        data.getHandle().release();
        writeToFile(aux);
        System.out.println("Done.");
    }
}
