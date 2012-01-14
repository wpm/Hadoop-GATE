package wpmcn.gate.hadoop;

import gate.*;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.GateException;
import gate.util.persistence.PersistenceManager;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * A wrapper for a GATE application
 *
 * This object parses arbitrary text using a GATE application supplied to the constructor.
 */
public class GATEApplication {
   private CorpusController application;
   private Corpus corpus;

   /**
    * Initialize the GATE application
    *
    * @param gateHome path to an GATE application directory containing an application.xgapp in its root
    * @throws GateException
    * @throws IOException
    */
   public GATEApplication(String gateHome) throws GateException, IOException {
      Gate.runInSandbox(true);
      Gate.setGateHome(new File(gateHome));
      Gate.setPluginsHome(new File(gateHome, "plugins"));
      Gate.init();
      URL applicationURL = new URL("file:" + new Path(gateHome, "application.xgapp").toString());
      application = (CorpusController) PersistenceManager.loadObjectFromUrl(applicationURL);
      corpus = Factory.newCorpus("Hadoop Corpus");
      application.setCorpus(corpus);
   }

   /**
    * Analyze text, returning annotations in XML
    * @param text the text to analyze
    * @return GATE XML annotation document
    * @throws ResourceInstantiationException
    * @throws ExecutionException
    */
   public String xmlAnnotation(String text) throws ResourceInstantiationException, ExecutionException {
      Document document = Factory.newDocument(text);
      annotateDocument(document);
      String xml = document.toXml();
      Factory.deleteResource(document);
      return xml;
   }

   @SuppressWarnings({"unchecked"}) // Needed for corpus.add(document)
   private Document annotateDocument(Document document) throws ResourceInstantiationException, ExecutionException {
      corpus.add(document);
      application.execute();
      corpus.clear();
      return document;
   }

   /**
    * Free resources associated with this object. This should be called before the object is deleted.
    */
   public void close() {
      Factory.deleteResource(corpus);
      Factory.deleteResource(application);
   }

   /**
    * Entry point that allows the GATE application to be run outside Hadoop. The first positional argument is an
    * archived GATE application file, and the second positional argument is a document. The document annotations are
    * written to standard out.
    *
    * @param args positional arguments
    * @throws Exception
    */
   static public void main(String[] args) throws Exception {
      String gateHome = args[0];
      String content = FileUtils.readFileToString(new File(args[1]));

      GATEApplication gate = new GATEApplication(gateHome);
      String annotation = gate.xmlAnnotation(content);
      System.out.println(annotation);
      gate.close();
   }
}
