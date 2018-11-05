/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}

//Flushes dirty pages and deallocates the buffer pool and BufDesc table
BufMgr::~BufMgr()
{

}

// Advances clock to next frame in buffer pool.
void BufMgr::advanceClock()
{
	clockHand = (clockHand == numBufs - 1) ? 0 : clockHand + 1;
}

// Allocates free frame using clock algorithm
// If necessary, writes dirty page back to disk
// Throws buffer_exceeded_exception if all buffer frames are pinned
// If buffer frame allocated has valid page in it, remove entry from hash table
void BufMgr::allocBuf(FrameId & frame)
{

}

// Check if page already in buffer pool and:
// 1. Page is not in buffer buffer pool
// Call allocBuf, file->readPage, insert into hashtable, invoke Set(), return pointer to frame
// 2. Page is in buffer pool
// set appropriate refbit, increment pinCnt, return pointer to frame containing the page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

}

// decrememnts pinCntof frame, if dirty == true sets dirty bit, throws page_not_pinned_exception if pinCnt == 0
// does nothing if page not in table lookup
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	FrameId frame_id;
	try {
		// Lookup hash
		hashTable->lookup(file, pageNo, frame_id);

		// find frame in table
		BufDesc frame = bufDescTable[frame_id];

		// throw page not pinned if not pinned
		if (frame.pinCnt <= 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frame_id)
		}

		// udpate dirty value
		if (dirty) {
			frame.dirty = true;
		}

		// decrement from being unpinned
		frame.pinCnt--;

	} catch (HashNotFoundException h) {
		// Do nothing
	}

}

// scans bufTable for pages belonging to file
// if page dirty, file-writePage() and then set dirty back to false
// remove page from hashtable
// invoke Clear() method of bufDesc for page frame
// throws page_pinned_exception if file pinned
// throws bad_buffer_exception if invalid page encountered
void BufMgr::flushFile(const File* file)
{

}

// allocate empty page in file
// call allocBuf
// entry inserted into hash table and Set()
// returns page number and pointer to buffer frame
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{

}

// deletes page from file
// if page to be deleted is allocated a frame in pool, free it and remove from hashtable
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frame_id;

	try {
		// lookup in hashtable
		hashTable->lookup(file, PageNo, frame_id);

		// if found, remove it and clear buffer frame
		hashTable->remove(file, PageNo);
		bufDescTable[frame_id].Clear();

	} catch (HashNotFoundException h) {
		// not found, ignore not found and delete
	}

	// delete page
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
		BufDesc* tmpbuf;
		int validFrames = 0;

		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tmpbuf = &(bufDescTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tmpbuf->Print();

			if (tmpbuf->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}

}
