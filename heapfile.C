#include "heapfile.h"
#include "error.h"
 
// routine to create a heapfile
const Status createHeapFile(const string fileName)
{

    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {  
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		Status createStatus = db.createFile(fileName);
        if(createStatus != OK){
            return createStatus;
        }

        // opens the file created
        Status openStatus = db.openFile(fileName, file);
        if(openStatus != OK){
            return openStatus;
        }

        // initiates the allocation of header page
        Status allocStatusHdr = bufMgr->allocPage(file, hdrPageNo, newPage);
        if(allocStatusHdr != OK){
            db.closeFile(file); // ensures no memory leak
            return allocStatusHdr;
        }
        hdrPage = (FileHdrPage*)newPage; // initializes header page


        // initiates the allocates of the new page 
        Status allocStatusPage = bufMgr->allocPage(file, newPageNo, newPage);
        if(allocStatusPage != OK){
            bufMgr->unPinPage(file, hdrPageNo, true);
            db.closeFile(file); // ensures no memory leak
            return allocStatusPage;
        }
        newPage->init(newPageNo); // initializes the new page


        // ...then initializes the header page's stats
        strcpy(hdrPage->fileName, fileName.c_str()); // sets file name
        hdrPage->firstPage = newPageNo; // sets first page#
        hdrPage->lastPage = newPageNo; // sets last page#
        hdrPage->pageCnt = 2; // header + first page = 2 pages
        hdrPage->recCnt = 0; // no records yet

        // ...and initializes the data page
        newPage->setNextPage(-1); // set there to be no next page

        // unpins the new page aside from header
        Status unpinStatus = bufMgr->unPinPage(file, newPageNo, true);
        if(unpinStatus != OK){
            bufMgr->unPinPage(file, hdrPageNo, true);
            db.closeFile(file); // ensure file closure
            return unpinStatus;
        }

        // unpins the header page
        Status unpinStatusHdr = bufMgr->unPinPage(file, hdrPageNo, true);
        if(unpinStatusHdr != OK){
            db.closeFile(file); // esure file closure
            return unpinStatusHdr;
        }

        // closes the file
        db.closeFile(file);
        return OK; // page creation successful
		
    }
    return (FILEEXISTS); // file exists
}




// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{   
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {

        int pageNo = -1; // placeholder page#

        // read header page
        Status firstStatus = filePtr->getFirstPage(pageNo);
        if(firstStatus!= OK){
            cerr <<"getFirstPage failed\n"; // if fails, displays error
            returnStatus = firstStatus; // ...and status
            return;
        }

        // reads the header page in Buffer Pool
        status = bufMgr->readPage(filePtr, pageNo, pagePtr);
        if(status != OK) {
            cerr << "readPage failed\n";
            returnStatus = status; // if fails, display error
            return;
        }

        // ...otherwise, initiates header page info
        headerPage = (FileHdrPage*)pagePtr; // cast header page to FileHdrPage
        headerPageNo = pageNo;// set page#
        hdrDirtyFlag = false; // sets header Page to not be updated

        // reads first data page (not header)
        curPageNo = headerPage->firstPage; // page# of data page
        status = bufMgr->readPage(filePtr, curPageNo, pagePtr); // reads page
        if(status != OK) {
            cerr << "readPage failed\n";
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            returnStatus = status; // if it fails, unpin header from BufferPool
            return;
        }

        // ...otherwise, initializes data page info stats
        curPage = pagePtr;  // sets current page ptr to data page
        curDirtyFlag = false; // marks data page as not updated
        curRec = NULLRID; // sets last record to null
        returnStatus = OK; // SUCCESS
        return;
    }
    else{ // otherwise, return status error if file fails to open
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}




// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file 
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    
    //checks if current page is not null & is page# is the same as record's page# (rid.pageNo) 
    if(curPage != NULL && curPageNo == rid.pageNo){
        status = curPage->getRecord(rid, rec); // gets record from current page
        if(status == OK){ 
            curRec=rid; // if successful in retrieving record, sets current recort to be record retrieved
        }
        return status; // otherwise returns status error
    }

    // checks if current page is not null
    if(curPage != NULL){
        // otherwise unpins current page
        Status unpinStatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if(unpinStatus !=OK){
            return unpinStatus;
        }
        // ... and sets current page stats to have null stats
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
    }

    // checks if record's page# is readable in buffer pool
    Status readStatus = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if(readStatus!=OK){
        return readStatus;
    }

    // sets current page stats
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    status = curPage->getRecord(rid, rec);
    if(status == OK){
        curRec = rid; // sets current record to be record retrieved if success in getting record
    }
    return status; 
}


HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    // scan till end of fils
    while(true){
        // checks if current page is null
        if(curPage == NULL){
            // sets page# to first data page
            curPageNo = headerPage->firstPage;

            // reads first data page
            if(curPageNo == -1){
                return FILEEOF; // if -1 --> end of file
            }

            // reads first data page to bufferpool
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if(status!=OK){
                return status;
            }
            // sets current page stats
            curDirtyFlag = false;
            curRec = NULLRID; // sets last record to null
        }

        // ensures that current record is not null (-1,-1 OR NULLRID)
        if(curRec.pageNo == -1 && curRec.slotNo == -1){
            status = curPage->firstRecord(nextRid); // sets next record to first record
        } else{
            status = curPage->nextRecord(curRec, nextRid); // or sets next record to next record if not null
        }

        // checks if next record is valid
        if(status==OK){
            curRec = nextRid; // sets current record to next record
            status = curPage->getRecord(curRec, rec); // retrieves record
            if(status!=OK){
                return status;
            }
            // checks if record matches 
            if(matchRec(rec)){
                outRid=curRec;
                return OK;
            } 
            continue; // if not, checks next record
        }

        // ensures that current record is not null or end of page
        if(status != NORECORDS && status != ENDOFPAGE){
            return status;
        }
        // checks if there is a next page
        status = curPage->getNextPage(nextPageNo);
        if(status != OK){
            return status;
        }
        // checks if next page is valid
        if(nextPageNo == -1){
            return FILEEOF;
        }
    
        // unpins current page because it no longer needed
        Status unpinStatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if(unpinStatus!=OK){
            return unpinStatus;
        }

        // sets current page stats to have null stats
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
        curRec = NULLRID;

        // reads next page from bufferpool
        curPageNo = nextPageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status!=OK){
            return status;
        }
        // sets current page stats to have null stats after 
        // ensuring that recoord to be not updated & is set to null
        curDirtyFlag=false;
        curRec=NULLRID;
    }
}



// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // if no the cur page is not null and is not the last page
    if(curPage==NULL || curPageNo != headerPage->lastPage){

        // cheks if current page is not null but is the last page
        if(curPage != NULL){
            // unpins current page
            unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if(unpinstatus!=OK){
                return unpinstatus;
            }
        }
        // ..otherwise, set curent page# to last data page
        curPageNo = headerPage->lastPage;
        // then reads last page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status != OK){
            return status;
        }
        // and sets current page stats
        curDirtyFlag = false;
    }

    // ...otherwise insert record to current page
    status = curPage->insertRecord(rec, rid);
    if(status==OK){
        // if success, then set outRid (output record) to be record inserted
        outRid = rid;
        // and also sets current record stats (as inserted)
        curDirtyFlag = true;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curRec = rid;
        return OK;
    }

    // ensires there is room for inserting new page
    if(status != NOSPACE){
        return status;
    }

    // allocates new page to bufferpool
    status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    if(status !=OK){
        return status;
    }

    // initiates new page and set next page to be null
    newPage->init(newPageNo);
    newPage->setNextPage(-1);

    // set current page's next page to be the new page
    curPage->setNextPage(newPageNo);
    curDirtyFlag=true;

    // sets last page to be new page
    headerPage->lastPage = newPageNo;
    headerPage->pageCnt++; // counts pages
    hdrDirtyFlag = true;

    // unpins current page becasue it is not needed
    unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    if(unpinstatus!=OK){
        return unpinstatus;
    }

    // sets current page stats to new page stats
    curPage = newPage;
    curPageNo = newPageNo;
    curDirtyFlag = false;

    // inserts record to new page
    status = curPage->insertRecord(rec, rid);
    if(status!=OK){
        return status;
    }

    // set output record to inserted record
    outRid = rid;
    curDirtyFlag = true;
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curRec=rid;
    return OK;
}


