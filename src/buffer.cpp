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

	// Flushes dirty pages and deallocates the buffer pool, BufDesc table, and hashtable
	BufMgr::~BufMgr()
	{
		// Flush any dirty pages
		for (uint32_t i = 0; i < numBufs; i++) {

			BufDesc currDesc = bufDescTable[i];
			if (currDesc.dirty && currDesc.valid) {
				currDesc.file->writePage(bufPool[currDesc.frameNo]);
			}
		}

		// Deallocate memory structures
		delete[] bufDescTable;
		delete[] bufPool;
		delete hashTable;
	}

	// Advances clock to next frame in buffer pool.
	void BufMgr::advanceClock()
	{
		clockHand = (clockHand + 1) % numBufs;
	}

	// Allocates free frame using clock algorithm
	// If necessary, writes dirty page back to disk
	// Throws buffer_exceeded_exception if all buffer frames are pinned
	// If buffer frame allocated has valid page in it, remove entry from hash table
	void BufMgr::allocBuf(FrameId &frame)
	{
		uint32_t origLoc = clockHand;
		BufDesc currDesc;
		bool found = false;
		uint32_t counter = 0;

		// Iterate through entries until we find a free frame, or
		// discover that they're all pinned
		while (!found && (counter < numBufs)) {

			advanceClock();

			// If we cycle back to start loc, clear count
			if (clockHand == origLoc) {
				counter = 0;
			}

			// Get description of current frame
			currDesc = bufDescTable[clockHand];

			// If valid is not set, use current frame
			if (!currDesc.valid) {

				frame = currDesc.frameNo;
				found = true;
			}
			// If valid and refbit set, clear refbit and advance clock
			else if (currDesc.refbit) {

				bufDescTable[clockHand].refbit = false;
			}
			// If frame is currently pinned increment count 
			else if (currDesc.pinCnt > 0) {
				counter++;
			}
			// If frame is valid and not pinned, use it.
			else {

				// If frame is dirty, write page to disk before using
				if (currDesc.dirty) {

					bufDescTable[clockHand].file->writePage(bufPool[currDesc.frameNo]);
				}

				// Remove frame from hash table
				hashTable->remove(currDesc.file, currDesc.pageNo);

				// Initialize frame for new data
				bufDescTable[clockHand].Clear();

				frame = currDesc.frameNo;
				found = true;
			}
		}

		// If buffer is full, throw exception
		if (!found) {
			throw BufferExceededException();
		}
	}

	// Check if page already in buffer pool and:
	// 1. Page is not in buffer buffer pool
	// Call allocBuf, file->readPage, insert into hashtable, invoke Set(), return pointer to frame
	// 2. Page is in buffer pool
	// set appropriate refbit, increment pinCnt, return pointer to frame containing the page
	void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
	{
		FrameId frameNo;
		try {
			// Fetch the desired hashtable
			hashTable->lookup(file, pageNo, frameNo);

			//  Set refbit to true
			bufDescTable[frameNo].refbit = true;

			// Inc pint count
			bufDescTable[frameNo].pinCnt++;

			// Return the page reference
			page = &bufPool[frameNo];

		}
		catch (HashNotFoundException h) {

			// If page is not in hashtable, which indicates buffer pool does not contain it
			// Therefore, we need to read from disk
			Page p = file->readPage(pageNo);

			// allocate buffer frame that will hold the page
			allocBuf(frameNo);

			// Add the page to buffer pool
			bufPool[frameNo] = p;

			// Insert record into hash table
			hashTable->insert(file, pageNo, frameNo);

			// Set appropriate frame attr
			bufDescTable[frameNo].Set(file, pageNo);

			// Return by page ref
			page = &bufPool[frameNo];
		}
	}

	// decrememnts pinCntof frame, if dirty == true sets dirty bit, throws page_not_pinned_exception if pinCnt == 0
	// does nothing if page not in table lookup
	void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
	{
		FrameId frame_id;

		// Lookup hash
		hashTable->lookup(file, pageNo, frame_id);

		// find frame in table
		BufDesc frame = bufDescTable[frame_id];

		if (frame.pinCnt <= 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frame_id);
		}

		// udpate dirty value
		if (dirty) {
			bufDescTable[frame_id].dirty = true;
		}

		// decrement from being unpinned
		bufDescTable[frame_id].pinCnt--;

	}

	// scans bufTable for pages belonging to file
	// if page dirty, file-writePage() and then set dirty back to false
	// remove page from hashtable
	// invoke Clear() method of bufDesc for page frame
	// throws page_pinned_exception if file pinned
	// throws bad_buffer_exception if invalid page encountered
	void BufMgr::flushFile(const File* file)
	{
		
		// Iterate through buffer and flush all frames belonging to current file
		for (uint32_t i = 0; i < numBufs; i++) {

			BufDesc currDesc = bufDescTable[i];

			if (currDesc.file == file) {

				// If not valid, throw a BadBufferException
				if (!currDesc.valid)
					throw BadBufferException(currDesc.frameNo, currDesc.dirty, currDesc.valid, currDesc.refbit);

				// If pinned, throw a PagePinnedException
				if (currDesc.pinCnt > 0)
					throw PagePinnedException(file->filename(), currDesc.pageNo, currDesc.frameNo);

				// If dirty, write to disk and clear dirty bit
				if (currDesc.dirty) {

					Page currPage = bufPool[currDesc.frameNo];
					bufDescTable[i].file->writePage(currPage);
					bufDescTable[i].dirty = false;
				}

				// Remove frame mapping from hash table and clear buffer location
				hashTable->remove(file, currDesc.pageNo);

				bufDescTable[i].Clear();
			}
		}
	}

	// allocate empty page in file
	// call allocBuf
	// entry inserted into hash table and Set()
	// returns page number and pointer to buffer frame
	void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
	{
		// Available frame (filled by allocBuf)
		FrameId frame;

		// Allocate new page
		Page currPage = file->allocatePage();

		// Allocate buffer frame
		allocBuf(frame);

		// Put page in buffer pool
		bufPool[frame] = currPage;

		// Add record to hashTable
		hashTable->insert(file, currPage.page_number(), frame);

		bufDescTable[frame].Set(file, currPage.page_number());

		pageNo = currPage.page_number();
		page = &bufPool[frame];
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

		}
		catch (HashNotFoundException h) {
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
			tmpbuf = &(bufDescTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tmpbuf->Print();

			if (tmpbuf->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}
}