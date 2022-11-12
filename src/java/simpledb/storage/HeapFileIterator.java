package simpledb.storage;

import java.util.Iterator;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator {

    private HeapFile heapFile;
    private TransactionId tid;
    private int nextPageNo;
    private Iterator<Tuple> tupleIterator;

    public HeapFileIterator(final HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.nextPageNo = 0;
        this.tupleIterator = this.getNextPageIterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if(this.tupleIterator == null) return null;

        if(this.tupleIterator.hasNext()) {
            return this.tupleIterator.next();
        } else if (this.nextPageNo < this.heapFile.numPages()) {
            this.tupleIterator = this.getNextPageIterator();

            return readNext();
        }

        return null;
    }

    public void close() {
        super.close();
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }

    private Iterator<Tuple> getNextPageIterator() throws DbException, TransactionAbortedException {
        return getNextPage().iterator();
    }

    private HeapPage getNextPage() throws DbException, TransactionAbortedException {
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.nextPageNo);
        this.nextPageNo++;
        return (HeapPage) Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_ONLY);
    }
}
