package cws.core.storage.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * Cache manager which uses Unlimited cache strategy for all files.
 */
public class UnlimitedCacheManager extends VMCacheManager {
    public UnlimitedCacheManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    private Map<VM, VMCache> cache = new HashMap<VM, UnlimitedCacheManager.VMCache>();

    /**
     * Since we use per-VM cache this inner class is convenient.
     */
    private class VMCache {
        // didn't use LinkedHashSet because it doesn't have push/poll methods
        private LinkedList<DAGFile> filesList = new LinkedList<DAGFile>();
        private Set<DAGFile> filesSet = new HashSet<DAGFile>();

        public VMCache(VM vm) {
           
        }

        /**
         * Puts the file to the local cache.
         */
        public void putFileToCache(DAGFile file) {
            filesSet.add(file);
            filesList.push(file);
                   
        }

        /**
         * @return true if the file is in the cache, false otherwise.
         */
        public boolean getFileFromCache(DAGFile file) {
            return filesSet.contains(file);
        }
    }

    @Override
    public void putFileToCache(DAGFile file, Job job) {
        if (cache.get(job.getVM()) == null) {
            cache.put(job.getVM(), new VMCache(job.getVM()));
        }
        cache.get(job.getVM()).putFileToCache(file);
    }

    @Override
    public boolean getFileFromCache(DAGFile file, Job job) {
        VMCache vmCache = cache.get(job.getVM());
        if (vmCache != null) {
            return vmCache.getFileFromCache(file);
        }
        return false;
    }
    
    @Override
    public boolean getFileFromCache(DAGFile file, VM vm) {
        VMCache vmCache = cache.get(vm);
        if (vmCache != null) {
            return vmCache.getFileFromCache(file);
        }
        return false;
    }
    
}
