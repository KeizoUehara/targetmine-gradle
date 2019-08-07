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

import org.apache.commons.lang.StringUtils;
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

        Statement stmt = connection.createStatement();

        // TODO: SQL 要確認
        String queryAllmut = "select * from hgmd_pro.allmut " +
                "LEFT JOIN hgmd_pro.mutnomen ON " +
                "hgmd_pro.allmut.acc_num = hgmd_pro.mutnomen.acc_num " +
                "LEFT JOIN hgmd_phenbase.hgmd_mutation ON " +
                "hgmd_pro.allmut.acc_num = hgmd_phenbase.hgmd_mutation.acc_num " +
                "LEFT JOIN hgmd_phenbase.phenotype_concept ON " +
                "hgmd_phenbase.hgmd_mutation.phen_id = hgmd_phenbase.phenotype_concept.phen_id " +
                // TODO : debug
                "limit 5 " +
                        ";";
        ResultSet resAllmut = stmt.executeQuery(queryAllmut);
        while (resAllmut.next()) {


            createHgmd(resAllmut);
            createPublication(resAllmut);


        }
    }

    private void createHgmd(ResultSet response) throws Exception {
        String identifer = response.getString("acc_num");
        String describe = response.getString("descr");
        String variantClass = response.getString("tag");

        Item item = createItem("Hgmd");
        item.setAttribute("identifier", identifer);
        item.setAttribute("describe", describe);
        item.setAttribute("variantClass", variantClass);
        store(item);
    }

    private void createPublication(ResultSet response) throws Exception {
        String pubMedId = response.getString("pmid");

        Item item = createItem("Publication");
        item.setAttribute("pubMedId", pubMedId);
        store(item);
    }

    private void createSnp(ResultSet response) throws Exception {
        String identifier = response.getString("dbsnp");
        if(identifier == null || identifier.length() == 0) {
            identifier = response.getString("acc_num");
        }

        String coodStart = response.getString("coordSTART");
        String chromosome = response.getString("chromosome");
        // TODO: データの作り方 要確認 : allmut.chromosomeとallmut.coordSTART
        String location = chromosome + coodStart;

        // TODO: データの作り方 要確認 : allmut.hgvsまたはallmut.deletionまたはallmut.insertion
        String refSnpAllele = "";
        if(response.getString("hgvs") != null && response.getString("hgvs").length() != 0) {
            refSnpAllele = response.getString("hgvs");
        }else if(response.getString("deletion") != null && response.getString("deletion").length() != 0) {
            refSnpAllele = response.getString("deletion");
        }else if(response.getString("insertion") != null && response.getString("insertion").length() != 0) {
            refSnpAllele = response.getString("insertion");
        }

        // TODO: データの作り方　要確認 :  ?
        String orientation = "";

        Item item = createItem("SNP");
        item.setAttribute("identifier", identifier);
        if (!StringUtils.isEmpty(location)) {
            item.setAttribute("location", location);
        }
        if (!StringUtils.isEmpty(chromosome)) {
            item.setAttribute("chromosome", chromosome);
        }
        if (!StringUtils.isEmpty(refSnpAllele)) {
            item.setAttribute("refSnpAllele", refSnpAllele);
        }
        if (!StringUtils.isEmpty(orientation)) {
            item.setAttribute("orientation", orientation);
        }

        store(item);
    }

    private void createGene(ResultSet response) throws Exception {
        // refCore
        String refCore = response.getString("refCORE");
        // TODO : refCoreを変換？
        String ncbiGeneId = refCore;

        Item item = createItem("Gene");
        item.setAttribute("ncbiGeneId", ncbiGeneId);
        store(item);
    }

//    private String createSNPReference(String mrnaAcc, String mrnaPos, String orientation,
//                                      String allele, String codon, String proteinAcc, int aaPos, String residue,
//                                      String funcRef, String vaItemRef) throws ObjectStoreException {
//        Item item = createItem("SNPReference");
//        item.setAttribute("mrnaAccession", mrnaAcc);
//        if (!StringUtils.isEmpty(mrnaPos)) {
//            item.setAttribute("mrnaPosition", mrnaPos);
//        }
//        if (!StringUtils.isEmpty(orientation)) {
//            item.setAttribute("orientation", orientation);
//        }
//        if (!StringUtils.isEmpty(allele)) {
//            item.setAttribute("mrnaAllele", allele);
//        }
//        if (!StringUtils.isEmpty(codon)) {
//            item.setAttribute("mrnaCodon", codon);
//        }
//        if (!StringUtils.isEmpty(proteinAcc)) {
//            item.setAttribute("proteinAccession", proteinAcc);
//        }
//        if (aaPos > 0) {
//            item.setAttribute("proteinPosition", String.valueOf(aaPos));
//        }
//        if (!StringUtils.isEmpty(residue)) {
//            item.setAttribute("residue", residue);
//        }
//        if (funcRef != null) {
//            item.setReference("function", funcRef);
//        }
//        item.setReference("annotation", vaItemRef);
//        store(item);
//
//        return item.getIdentifier();
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }
}
