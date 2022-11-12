package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.numPages = (int) f.length() / BufferPool.getPageSize();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        long pageOffset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];

        if (pid.getPageNumber() > this.numPages()) {
            throw new NoSuchElementException();
        }

        try {
            // read the existing page from disk
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.f, "r");
            randomAccessFile.seek(pageOffset);
            randomAccessFile.read(data);
            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();

        this.numPages = Math.max(this.numPages, pid.getPageNumber() + 1);

        long pageOffset = pid.getPageNumber() * BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.f, "rw");
        randomAccessFile.seek(pageOffset);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> modifiedPages = new ArrayList<>();

        HeapPage pageWithSpace = null;

        for(int i = 0; i < this.numPages(); ++i) {
            HeapPageId pageId = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                pageWithSpace = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                break;
            } 
        }

        // 1. can find
        if(pageWithSpace != null) {
            pageWithSpace.insertTuple(t);
            modifiedPages.add(pageWithSpace);
            return modifiedPages;
        } 

        // 2. cannot find
        HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        modifiedPages.add(newPage);

        this.numPages += 1;

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        ArrayList<Page> modifiedPages = new ArrayList<>();

        HeapPage pageToDeleteFrom = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        pageToDeleteFrom.deleteTuple(t);

        modifiedPages.add(pageToDeleteFrom);

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

