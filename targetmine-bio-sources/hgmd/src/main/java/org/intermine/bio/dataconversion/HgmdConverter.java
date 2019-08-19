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
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.sql.Database;
import org.intermine.xml.full.Item;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author
 */
public class HgmdConverter extends BioDBConverter
{
    // 
    private static final String DATASET_TITLE = "hgmd";
    private static final String DATA_SOURCE_NAME = "hgmd";

    private Map<String, String> geneMap = new HashMap<String, String>();
    private Map<String, String> snpMap = new HashMap<String, String>();
    private Map<String, String> publicationMap = new HashMap<String, String>();

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
//            createSnpFunction(resAllmut);
//            createGene(resAllmut);

        }

        stmt.close();
        connection.close();
    }

    private void createHgmd(ResultSet response) throws Exception {
        String identifier = response.getString("acc_num");
        String description = response.getString("descr");
        String variantClass = response.getString("tag");

        Item item = createItem("Hgmd");
        item.setAttribute("identifier", identifier);
        item.setAttribute("description", description);
        item.setAttribute("variantClass", variantClass);
        item.addToCollection("publications", getPublication(response.getString("pmid")));
        item.addToCollection("sNP", getSnp(response));

        store(item);
    }

    private String getPublication(String pubMedId) throws ObjectStoreException {
        String ret = publicationMap.get(pubMedId);

        if (ret == null) {
            // Publication set only pubMedId.
            Item item = createItem("Publication");
            item.setAttribute("pubMedId", pubMedId);
            store(item);
            ret = item.getIdentifier();
            geneMap.put(pubMedId, ret);
        }
        return ret;
    }

    private String getSnp(ResultSet response) throws Exception {
        String identifier = response.getString("dbsnp");
        if(identifier == null || identifier.length() == 0) {
            identifier = response.getString("acc_num");
        }

        String coodStart = response.getString("startCoord");
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

        String ret = snpMap.get(identifier);
        if (ret == null) {
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
            ret = item.getIdentifier();
            snpMap.put(identifier, ret);
        }
        return ret;
    }

    private void createSnpFunction(ResultSet response) throws Exception {
        String name = "";
        String description = "";

        Item item = createItem("SNPFunction");
        if (!StringUtils.isEmpty(name)) {
            item.setAttribute("name", name);
        }
        if (!StringUtils.isEmpty(description)) {
            item.setAttribute("description", description);
        }

        store(item);
    }

    private String createGene(ResultSet response) throws Exception {
        // refCore
        String refCore = response.getString("refCORE");
        // TODO : refCoreを変換？
        String ncbiGeneId = refCore;

        // retval : gene.identifier
        return getGene(ncbiGeneId);
    }

    private String getGene(String ncbiGeneId) throws ObjectStoreException {
        String ret = geneMap.get(ncbiGeneId);

        if (ret == null) {
            Item item = createItem("Gene");
            item.setAttribute("primaryIdentifier", ncbiGeneId);
            item.setAttribute("ncbiGeneId", ncbiGeneId);
            store(item);
            ret = item.getIdentifier();
            geneMap.put(ncbiGeneId, ret);
        }
        return ret;
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
