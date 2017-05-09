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
import java.io.IOException;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.EDDifferentialPrivacy;
import org.deidentifier.arx.criteria.HierarchicalDistanceTCloseness;

/**
 * This class demonstrates how to use (e,d)-DP, reading data from Cassandra.
 *
 * @author Karol Babioch
 * @author Fabian Prasser
 */
public class Example54 extends Example {

    /**
     * Main entry point.
     *
     * @param args
     * @throws IOException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException
    {
        anonymizeVoters();
    }


    public static void anonymizeVoters() throws IOException, ClassNotFoundException
    {
        //establish connection
        DataSource source = DataSource.createCassandraSource("127.0.0.1","test","voters");
        //add columns 
        source.addColumn("id","id",DataType.INTEGER);
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
        //create data source
        Data data = Data.create(source);
        //print input
        //System.out.println("\n---------------------------------INPUT---------------------------------\n");
        //print(data.getHandle());
        System.out.println("\n");
        //build hierarchies for each non insensitive attribute
        AttributeType.Hierarchy voterStatHierarchy= AttributeType.Hierarchy.create(new File("src/voters_hierarchies/voter_stat_hierarchy.csv"), StandardCharsets.UTF_8,',');
        AttributeType.Hierarchy zipCodeHierarchy= AttributeType.Hierarchy.create(new File("src/voters_hierarchies/zipcode_hierarchy.csv"), StandardCharsets.UTF_8,',');
        AttributeType.Hierarchy cityHierarchy= AttributeType.Hierarchy.create(new File("src/voters_hierarchies/cities_hierarchy.csv"), StandardCharsets.UTF_8,',');
        AttributeType.Hierarchy birthStateHierarchy= AttributeType.Hierarchy.create(new File("src/voters_hierarchies/birth_state_hierarchy.csv"), StandardCharsets.UTF_8,',');
        //set risk of re-identification for each attribute
        data.getDefinition().setAttributeType("id", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("voter_stat", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("city", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("zip_code", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("race_code", AttributeType.SENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("ethnic_code", AttributeType.SENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("party_cd", AttributeType.SENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("gender_code", AttributeType.SENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("birth_place", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("drivers_lic", AttributeType.SENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("age", AttributeType.INSENSITIVE_ATTRIBUTE);          //insensitive because it is already an interval
        //set hierarchies
        data.getDefinition().setHierarchy("voter_stat", voterStatHierarchy);
        data.getDefinition().setHierarchy("zip_code", zipCodeHierarchy);
        data.getDefinition().setHierarchy("city", cityHierarchy);
        data.getDefinition().setHierarchy("birth_place",birthStateHierarchy);
        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        // Create a differential privacy criterion (epsilon,delta) - delta is usually <=10^(-4)
        EDDifferentialPrivacy criterion = new EDDifferentialPrivacy(1,0.0001, DataGeneralizationScheme.create(data, DataGeneralizationScheme.GeneralizationDegree.MEDIUM));
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(criterion);
        config.setMaxOutliers(1d);
        //Privacy models that protect data from attribute disclosure, must be assigned to sensitive attributes(party_cd)
        config.addCriterion(new HierarchicalDistanceTCloseness("race_code", 0.2d, AttributeType.Hierarchy.create(new File("src/voters_hierarchies/race_code_hierarchy.csv"),StandardCharsets.UTF_8,',')));
        config.addCriterion(new HierarchicalDistanceTCloseness("ethnic_code", 0.2d, AttributeType.Hierarchy.create(new File("src/voters_hierarchies/ethnic_code_hierarchy.csv"),StandardCharsets.UTF_8,',')));
        config.addCriterion(new HierarchicalDistanceTCloseness("party_cd", 0.2d, AttributeType.Hierarchy.create(new File("src/voters_hierarchies/party_cd_hierarchy.csv"),StandardCharsets.UTF_8,',')));
        config.addCriterion(new HierarchicalDistanceTCloseness("gender_code", 0.2d, AttributeType.Hierarchy.create(new File("src/voters_hierarchies/gender_code_hierarchy.csv"),StandardCharsets.UTF_8,',')));
        config.addCriterion(new HierarchicalDistanceTCloseness("drivers_lic", 0.2d, AttributeType.Hierarchy.create(new File("src/voters_hierarchies/drivers_lic_hierarchy.csv"),StandardCharsets.UTF_8,',')));
        //anonymize input
        ARXResult result = anonymizer.anonymize(data, config);
        // print output
        //System.out.println("\n---------------------------------OUTPUT---------------------------------\n");
        DataHandle optimal = result.getOutput();
        //printHandle(optimal);
        File anonymizedData = new File("voters_anonymized.csv");
        optimal.save(anonymizedData,',');
    }
}
