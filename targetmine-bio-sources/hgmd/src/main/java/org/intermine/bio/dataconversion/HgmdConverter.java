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
import org.intermine.model.InterMineObject;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreFactory;
import org.intermine.objectstore.query.*;
import org.intermine.sql.Database;
import org.intermine.xml.full.Item;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * Integrate dbsnp in advance.
 * Read hgmd database in mysql.
 * mysql db setting wrote '~/.intermine/targetmine.properties'.
 * @author
 */
public class HgmdConverter extends BioDBConverter
{
    private static final Logger LOG = LogManager.getLogger(HgmdConverter.class);
    // 
    private static final String DATASET_TITLE = "hgmd";
    private static final String DATA_SOURCE_NAME = "hgmd";

    private Map<String, String> geneMap = new HashMap<String, String>();
    private Map<String, String> snpMap = new HashMap<String, String>();
    private Map<String, String> publicationMap = new HashMap<String, String>();

    private String osAlias = null;
    private Map<String, String> snpFunctionNameMap = new HashMap<String, String>();
    private Set<String> snpFunctionNameSet = new HashSet<String>();

    private static Map<String, String> mutypeToSnpFunctionNames = new HashMap<String, String>();
    static {
        Map<String, String> p = new HashMap<>();
        // key:hgmd_pro.allmut.mutype , value:snpfunction.name
        mutypeToSnpFunctionNames.put("frameshift", "frameshift");
        mutypeToSnpFunctionNames.put("missense", "missense");
        mutypeToSnpFunctionNames.put("nonsense", "STOP-GAIN");
        mutypeToSnpFunctionNames.put("nonstop", "STOP-LOSS");
        mutypeToSnpFunctionNames.put("synonymous", "cds-synon");
    }

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
        getSnpFunctionNames();

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
                "limit 10 " +
                        ";";
        ResultSet resAllmut = stmt.executeQuery(queryAllmut);
        while (resAllmut.next()) {
            createHgmd(resAllmut);
            String snpFunctionIdentifer = getOrCreateSnpFunction(resAllmut);
            LOG.info("snpFunctionIdentifer : " + snpFunctionIdentifer );
            getSNPReference(resAllmut, snpFunctionIdentifer, "");
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
        LOG.warn("getPublication : " + pubMedId );
        String ret = publicationMap.get(pubMedId);

        if (ret == null) {
            // Publication set only pubMedId.
            Item item = createItem("Publication");
            item.setAttribute("pubMedId", pubMedId);
            store(item);
            ret = item.getIdentifier();
            LOG.warn(" getPublication : pubmedid = "+ pubMedId +"publication identifer " + ret);
            geneMap.put(pubMedId, ret);
        }
        return ret;
    }

    private String getSnp(ResultSet response) throws Exception {
        String identifier = response.getString("dbsnp");
        LOG.warn("getSnp : dbsnp identifier " + identifier);
        if(identifier == null || identifier.length() == 0) {
            identifier = response.getString("acc_num");
            LOG.warn("getSnp : accnum identifier " + identifier);
        }

        String coodStart = response.getString("startCoord");
        LOG.warn("getSnp : coodStart " + coodStart);
        String chromosome = response.getString("chromosome");
        LOG.warn("getSnp : chromosome " + chromosome);
        // TODO: データの作り方 要確認 : allmut.chromosomeとallmut.coordSTART
        String location = chromosome + coodStart;

        // TODO: データの作り方 要確認 : allmut.hgvsまたはallmut.deletionまたはallmut.insertion
        String refSnpAllele = "";
        if(response.getString("hgvs") != null && response.getString("hgvs").length() != 0) {
            refSnpAllele = response.getString("hgvs");
            LOG.warn("getSnp : hgvs refSnpAllele " + refSnpAllele);
        }else if(response.getString("deletion") != null && response.getString("deletion").length() != 0) {
            refSnpAllele = response.getString("deletion");
            LOG.warn("getSnp : deletion refSnpAllele " + refSnpAllele);
        }else if(response.getString("insertion") != null && response.getString("insertion").length() != 0) {
            refSnpAllele = response.getString("insertion");
            LOG.warn("getSnp : insertion refSnpAllele " + refSnpAllele);
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
        LOG.warn("getSnp : ret " + ret);
        return ret;
    }

    /**
     * DB read SNPFunction column "name".
     * @throws Exception
     */
    private void getSnpFunctionNames() throws Exception {
        LOG.info("Start loading snpfunction name");
        ObjectStore os = ObjectStoreFactory.getObjectStore(osAlias);

        Query q = new Query();
        QueryClass qcSnpFunction = new QueryClass(os.getModel().
                getClassDescriptorByName("SNPFunction").getType());

        q.addFrom(qcSnpFunction);
        q.addToSelect(qcSnpFunction);

        Results results = os.execute(q);
        Iterator<Object> iterator = results.iterator();

        LOG.info("iterator before" );
        while (iterator.hasNext()) {
            LOG.info("iterator start" );
            ResultsRow<InterMineObject> rr = (ResultsRow<InterMineObject>) iterator.next();
            InterMineObject p = rr.get(0);

            LOG.info("InterMineObject { p :"+ p + "}" );
            String name = (String) p.getFieldValue("name");
            LOG.info(" loaded snpFunction { name : " + name + "}" );

            if (name != null) {
                snpFunctionNameSet.add(name);
            }
        }
        LOG.info("loaded "+ snpFunctionNameSet.size()+" snpFunction (size)" );
    }

    private String getOrCreateSnpFunction(ResultSet response) throws Exception {
        String mutype = response.getString("mutype");

        if(StringUtils.isEmpty(mutype)) {
            LOG.info("SNPFunction is Emptiy.");
            return "";
        }

        // mapping mutype -> snpfunction name.
        String snpFunctionName = mutypeToSnpFunctionNames.get(mutype);
        if(snpFunctionName == null) {
            // not mapping. use mutype.
            snpFunctionName = mutype;
        }

        String ret = snpFunctionNameMap.get(snpFunctionName);
        if(ret == null) {
            // create item.
            Item item = createItem("SNPFunction");
            item.setAttribute("name", snpFunctionName);
            store(item);
            ret = item.getIdentifier();
            LOG.info(" create SNPFunction! name = "+ snpFunctionName + "item identifier = " + ret);
            snpFunctionNameMap.put(snpFunctionName, item.getIdentifier());
        }

        LOG.info(" snpfunction() end. getIdentifier = " + ret);
        return ret;
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

    private String getSNPReference(ResultSet response, String snpFunctionRef, String variationAnnotationRef) throws Exception {
        String mrnaAcc = response.getString("refCORE") + response.getString("refVER"); // TODO: 文字列結合の方法?
        String mrnaPos = response.getString("cSTART");

        String orientation = ""; // TODO: データの作り方?
        String allele = response.getString("wildBASE") + response.getString("mutBASE"); // TODO: 文字列結合の方法?
        String codon = ""; // TODO: データの作り方?
        String proteinAcc = response.getString("protCORE") + response.getString("protVER"); // TODO: 文字列結合の方法?
        int aaPos = response.getInt("codon");
        String residue = response.getString("wildAMINO")  +  response.getString("mutAMINO"); // TODO: 文字列結合の方法?
        String funcRef = snpFunctionRef;
        String vaItemRef = variationAnnotationRef;

        return createSNPReference(mrnaAcc, mrnaPos, orientation, allele, codon, proteinAcc, aaPos, residue, funcRef, vaItemRef);
    }

    private String createSNPReference(String mrnaAcc, String mrnaPos, String orientation,
                                      String allele, String codon, String proteinAcc, int aaPos, String residue,
                                      String funcRef, String vaItemRef) throws ObjectStoreException {
        Item item = createItem("SNPReference");
        item.setAttribute("mrnaAccession", mrnaAcc);
        if (!StringUtils.isEmpty(mrnaPos)) {
            item.setAttribute("mrnaPosition", mrnaPos);
        }
        if (!StringUtils.isEmpty(orientation)) {
            item.setAttribute("orientation", orientation);
        }
        if (!StringUtils.isEmpty(allele)) {
            item.setAttribute("mrnaAllele", allele);
        }
        if (!StringUtils.isEmpty(codon)) {
            item.setAttribute("mrnaCodon", codon);
        }
        if (!StringUtils.isEmpty(proteinAcc)) {
            item.setAttribute("proteinAccession", proteinAcc);
        }
        if (aaPos > 0) {
            item.setAttribute("proteinPosition", String.valueOf(aaPos));
        }
        if (!StringUtils.isEmpty(residue)) {
            item.setAttribute("residue", residue);
        }
        if (funcRef != null) {
            item.setReference("function", funcRef);
        }
        if (vaItemRef != null) {
            item.setReference("annotation", vaItemRef);
        }

        store(item);

        return item.getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }

    public void setOsAlias(String osAlias) {
        this.osAlias = osAlias;
    }
}
