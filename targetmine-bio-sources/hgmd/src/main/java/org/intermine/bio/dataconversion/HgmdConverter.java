package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2019 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.sql.Database;
import org.intermine.xml.full.Item;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author
 */
public class HgmdConverter extends BioDBConverter
{
    // 
    private static final String DATASET_TITLE = "hgmd";
    private static final String DATA_SOURCE_NAME = "hgmd";


    /**
     * Construct a new HgmdConverter.
     * @param database the database to read from
     * @param model the Model used by the object store we will write to with the ItemWriter
     * @param writer an ItemWriter used to handle Items created
     */
    public HgmdConverter(Database database, Model model, ItemWriter writer) {
        super(database, model, writer, DATA_SOURCE_NAME, DATASET_TITLE);
    }


    /**
     * {@inheritDoc}
     */
    public void process() throws Exception {
        // a database has been initialised from properties starting with db.hgmd

        Connection connection = getDatabase().getConnection();

        // process data with direct SQL queries on the source database, for example:
        
        // Statement stmt = connection.createStatement();
        // String query = "select column from table;";
        // ResultSet res = stmt.executeQuery(query);
        // while (res.next()) {
        // }
        Statement stmt = connection.createStatement();
        String queryAllmut = "select * from allmut limit 5";
        ResultSet resAllmut = stmt.executeQuery(queryAllmut);
        while (resAllmut.next()) {
            String disease = resAllmut.getString("disease");
            String gene = resAllmut.getString("gene");

            Item item = createItem("Hgmd");
            item.setAttribute("disease", disease);
            item.setAttribute("gene", gene);
            store(item);

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }
}
