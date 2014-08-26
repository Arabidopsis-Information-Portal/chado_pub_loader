package chado;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
public class ChadoCV {
	private final String cvName;
    private final Map<Integer, ChadoCVTerm> termMap = new HashMap<Integer, ChadoCVTerm>();
    /**
     * Create a new ChadoCV.
     * @param cvName the name of the cv in chado that this object represents.
     */
    public ChadoCV(String cvName) {
        this.cvName = cvName;
    }

    /**
     * Return the cvName that was passed to the constructor.
     * @return the cv name
     */
    public final String getCvName() {
        return cvName;
    }
    public Map<Integer, ChadoCVTerm> getTerms(){
    	return termMap;
    }
    /**
     * Add a cvterm and its chado id to this cv.
     * @param cvtermId the chado id = cvterm.cvterm_id
     * @param chadoCvTerm the ChadoCVTerm object
     */
    public void addByChadoId(Integer cvtermId, ChadoCVTerm chadoCvTerm) {
        termMap.put(cvtermId, chadoCvTerm);
    }
    /**
     * Return the ChadoCVTerm object for a given cvterm_id.
     * @param cvtermId the chado id = cvterm.cvterm_id
     * @return the ChadoCVTerm
     */
    public ChadoCVTerm getByChadoId(Integer cvtermId) {
        return termMap.get(cvtermId);
    }

    /**
     * Return a Set of the root CVTerms in this CV - ie. those with no parents.
     * @return the cvterms
     */
    public Set<ChadoCVTerm> getRootCVTerms() {
        HashSet<ChadoCVTerm> rootTerms = new HashSet<ChadoCVTerm>();

        for (ChadoCVTerm cvterm: termMap.values()) {
            if (cvterm.getDirectParents().size() == 0) {
                rootTerms.add(cvterm);
            }
        }

        return rootTerms;
    }

    /**
     * Return a Set of all the CVTerms in this CV.
     * @return the cvterms
     */
    public Set<ChadoCVTerm> getAllCVTerms() {
        return new HashSet<ChadoCVTerm>(termMap.values());
    }

}

