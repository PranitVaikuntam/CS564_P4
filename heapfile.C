#include "heapfile.h"
#include "error.h"

const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    status = db.openFile(fileName, file);
    if (status != OK)
    {
		status = db.createFile(fileName);
		if (status != OK) return status;

		status = db.openFile(fileName, file);
		if (status != OK) return status;

		status = bufMgr->allocPage(file, hdrPageNo, newPage);
		if (status != OK) {
			db.closeFile(file);
			return status;
		}

		hdrPage = (FileHdrPage*) newPage;
		strcpy(hdrPage->fileName, fileName.c_str());

		status = bufMgr->allocPage(file, newPageNo, newPage);
		if (status != OK) {
			bufMgr->unPinPage(file, hdrPageNo, false);
			db.closeFile(file);
			return status;
		}

		newPage->init(newPageNo);

		hdrPage->firstPage = newPageNo;
		hdrPage->lastPage = newPageNo;
		hdrPage->pageCnt = 1;
		hdrPage->recCnt = 0;

		status = bufMgr->unPinPage(file, hdrPageNo, true);
		if (status != OK) {
			bufMgr->unPinPage(file, newPageNo, false);
			db.closeFile(file);
			return status;
		}

		status = bufMgr->unPinPage(file, newPageNo, true);
		if (status != OK) {
			db.closeFile(file);
			return status;
		}

		status = db.closeFile(file);
		return status;
    }
    return (FILEEXISTS);
}

const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile(fileName));
}

HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		status = filePtr->getFirstPage(headerPageNo);
		if (status != OK) {
			cerr << "failed to get first page\n";
			returnStatus = status;
			db.closeFile(filePtr);
			return;
		}

		status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
		if (status != OK) {
			cerr << "failed to read header page\n";
			returnStatus = status;
			db.closeFile(filePtr);
			return;
		}

		headerPage = (FileHdrPage*) pagePtr;
		hdrDirtyFlag = false;

		curPageNo = headerPage->firstPage;
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) {
			cerr << "failed to read first data page\n";
			bufMgr->unPinPage(filePtr, headerPageNo, false);
			returnStatus = status;
			db.closeFile(filePtr);
			return;
		}

		curDirtyFlag = false;
		curRec = NULLRID;
		returnStatus = OK;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }

    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";

	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print(status);
    }
}

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    if (curPageNo == rid.pageNo && curPage != NULL)
    {
    	status = curPage->getRecord(rid, rec);
    	curRec = rid;
    	return status;
    }
    else
    {
    	if (curPage != NULL)
    	{
    		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    		if (status != OK) return status;
    	}

    	curPageNo = rid.pageNo;
    	status = bufMgr->readPage(filePtr, curPageNo, curPage);
    	if (status != OK) return status;

    	curDirtyFlag = false;

    	status = curPage->getRecord(rid, rec);
    	curRec = rid;
    	return status;
    }
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
    if (!filter_) {
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int)) ||
         (type_ == FLOAT && length_ != sizeof(float))) ||
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
		curPageNo = markedPageNo;
		curRec = markedRec;
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false;
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
    Record  rec;

    if (curPage == NULL)
    {
    	curPageNo = headerPage->firstPage;
    	status = bufMgr->readPage(filePtr, curPageNo, curPage);
    	if (status != OK) return status;
    	curDirtyFlag = false;

    	status = curPage->firstRecord(tmpRid);
    	if (status == NORECORDS)
    	{
    		status = curPage->getNextPage(nextPageNo);
    		if (status != OK) return status;

    		if (nextPageNo == -1) return FILEEOF;

    		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    		if (status != OK) return status;

    		curPageNo = nextPageNo;
    		status = bufMgr->readPage(filePtr, curPageNo, curPage);
    		if (status != OK) return status;
    		curDirtyFlag = false;

    		status = curPage->firstRecord(tmpRid);
    		if (status != OK) return status;
    	}
    	else if (status != OK)
    	{
    		return status;
    	}
    }
    else
    {
    	status = curPage->nextRecord(curRec, tmpRid);

    	if (status == ENDOFPAGE)
    	{
    		status = curPage->getNextPage(nextPageNo);
    		if (status != OK) return status;

    		if (nextPageNo == -1) return FILEEOF;

    		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    		if (status != OK) return status;

    		curPageNo = nextPageNo;
    		status = bufMgr->readPage(filePtr, curPageNo, curPage);
    		if (status != OK) return status;
    		curDirtyFlag = false;

    		status = curPage->firstRecord(tmpRid);
    		if (status == NORECORDS)
    		{
    			return scanNext(outRid);
    		}
    		else if (status != OK)
    		{
    			return status;
    		}
    	}
    	else if (status != OK)
    	{
    		return status;
    	}
    }

    while (true)
    {
    	status = curPage->getRecord(tmpRid, rec);

    	if (status != OK)
    	{
    		status = curPage->nextRecord(tmpRid, nextRid);
    		if (status == ENDOFPAGE)
    		{
    			status = curPage->getNextPage(nextPageNo);
    			if (status != OK) return status;

    			if (nextPageNo == -1) return FILEEOF;

    			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    			if (status != OK) return status;

    			curPageNo = nextPageNo;
    			status = bufMgr->readPage(filePtr, curPageNo, curPage);
    			if (status != OK) return status;
    			curDirtyFlag = false;

    			status = curPage->firstRecord(tmpRid);
    			if (status == NORECORDS)
    			{
    				return scanNext(outRid);
    			}
    			else if (status != OK)
    			{
    				return status;
    			}
    		}
    		else if (status == OK)
    		{
    			tmpRid = nextRid;
    		}
    		else
    		{
    			return status;
    		}
    		continue;
    	}

    	if (matchRec(rec))
    	{
    		curRec = tmpRid;
    		outRid = tmpRid;
    		return OK;
    	}

    	status = curPage->nextRecord(tmpRid, nextRid);
    	if (status == ENDOFPAGE)
    	{
    		status = curPage->getNextPage(nextPageNo);
    		if (status != OK) return status;

    		if (nextPageNo == -1) return FILEEOF;

    		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    		if (status != OK) return status;

    		curPageNo = nextPageNo;
    		status = bufMgr->readPage(filePtr, curPageNo, curPage);
    		if (status != OK) return status;
    		curDirtyFlag = false;

    		status = curPage->firstRecord(tmpRid);
    		if (status == NORECORDS)
    		{
    			continue;
    		}
    		else if (status != OK)
    		{
    			return status;
    		}
    	}
    	else if (status == OK)
    	{
    		tmpRid = nextRid;
    	}
    	else
    	{
    		return status;
    	}
    }
}


const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

const Status HeapFileScan::deleteRecord()
{
    Status status;

    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}


const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    if (!filter) return true;

    if ((offset + length - 1) >= rec.length)
	return false;

    float diff = 0;
    switch(type) {

    case INTEGER:
        int iattr, ifltr;
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;
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
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status;
    RID		rid;

    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        return INVALIDRECLEN;
    }

    if (curPage == NULL)
    {
    	curPageNo = headerPage->lastPage;
    	status = bufMgr->readPage(filePtr, curPageNo, curPage);
    	if (status != OK) return status;
    	curDirtyFlag = false;
    }

    status = curPage->insertRecord(rec, rid);

    if (status == OK)
    {
    	outRid = rid;
    	curRec = rid;

    	headerPage->recCnt++;
    	hdrDirtyFlag = true;
    	curDirtyFlag = true;

    	return OK;
    }
    else if (status == NOSPACE)
    {
    	status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    	if (status != OK) return status;

    	newPage->init(newPageNo);

    	status = curPage->setNextPage(newPageNo);
    	if (status != OK)
    	{
    		bufMgr->unPinPage(filePtr, newPageNo, false);
    		return status;
    	}

    	headerPage->lastPage = newPageNo;
    	headerPage->pageCnt++;
    	hdrDirtyFlag = true;

    	status = bufMgr->unPinPage(filePtr, curPageNo, true);
    	if (status != OK)
    	{
    		bufMgr->unPinPage(filePtr, newPageNo, false);
    		return status;
    	}

    	curPage = newPage;
    	curPageNo = newPageNo;
    	curDirtyFlag = false;

    	status = curPage->insertRecord(rec, rid);
    	if (status != OK) return status;

    	outRid = rid;
    	curRec = rid;
    	headerPage->recCnt++;
    	hdrDirtyFlag = true;
    	curDirtyFlag = true;

    	return OK;
    }
    else
    {
    	return status;
    }
}
