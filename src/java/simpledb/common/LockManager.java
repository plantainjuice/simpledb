package simpledb.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.print.attribute.standard.Sides;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    private Map<TransactionId, Set<PageId>> txn2page;
    private Map<PageId, TransactionId> page2txn_ex; // exclude
    private Map<PageId, Set<TransactionId>> page2txn_sh; // share

    public LockManager() {
        txn2page = new ConcurrentHashMap<>();
        page2txn_ex = new ConcurrentHashMap<>();
        page2txn_sh = new ConcurrentHashMap<>();
    }

    public synchronized void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if(perm == Permissions.READ_ONLY) {
            _sLock(tid, pid); // S-lock
        } else {
            _xLock(tid, pid); // X-lock
        }

        Set<PageId> pids = txn2page.getOrDefault(tid, new HashSet<>());
        pids.add(pid);
        txn2page.put(tid, pids);
    }

    private synchronized void _sLock (TransactionId tid, PageId pid) throws TransactionAbortedException {
        // check X-lock
        TransactionId xtid = _getXLock(pid);
        if (xtid != null) {
            if (xtid.equals(tid) == false)
                throw new TransactionAbortedException();

            return;
        }

        Set<TransactionId> stids = _getSLock(pid);
        stids.add(tid);
        page2txn_sh.put(pid, stids);
    }

    private synchronized void _xLock (TransactionId tid, PageId pid) throws TransactionAbortedException {
        // check X-lock
        TransactionId xtid = _getXLock(pid);
        if (xtid != null) {
            if (xtid.equals(tid) == false)
                throw new TransactionAbortedException();

            return;
        }

        Set<TransactionId> stids = _getSLock(pid);
        // more than 1 S-lock
        if (stids.size() > 1)
            throw new TransactionAbortedException();

        // check upgrade
        if (stids.size() == 1 && stids.contains(tid) == false)
            throw new TransactionAbortedException();

        stids.clear();

        page2txn_ex.put(pid, tid);
    }

    public synchronized void unLock(TransactionId tid, PageId pid) {
        if(hasLock(tid, pid) == false) return;

        Set<PageId> pids = txn2page.get(tid);
        pids.remove(pid);

        // release X-lock
        if (page2txn_ex.containsKey(pid) ) {
            page2txn_ex.remove(pid);
        } else {
        // release S-lock
            Set<TransactionId> tids = page2txn_sh.get(pid);
            tids.remove(tid);
        }
    }

    public synchronized boolean hasLock(TransactionId tid, PageId pid) {
        Set<PageId> pids = txn2page.get(tid);

        if (txn2page.get(tid) == null)
            return false;

        return pids.contains(pid);
    }

    public synchronized void releaseLocksOnTransaction(TransactionId tid) {
        Set<PageId> pids = txn2page.get(tid);
        if (pids == null)
            return;

        Set<PageId> toRelease = new HashSet<>(pids);

        for(PageId pid : toRelease)
            unLock(tid, pid);
    }

    private synchronized Set<TransactionId> _getSLock(PageId pid) {
        return page2txn_sh.getOrDefault(pid, new HashSet<>());
    }

    private synchronized TransactionId _getXLock(PageId pid) {
        return page2txn_ex.getOrDefault(pid, null);
    }

    public synchronized Set<PageId> getPids(TransactionId tid) {
        Set<PageId> pids = txn2page.getOrDefault(tid, new HashSet<>());
        return pids;
    }
}
