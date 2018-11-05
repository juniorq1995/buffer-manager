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
	void BufMgr::allocBuf(FrameId &frame)
	{
		int origLoc = clockHand;
		BufDesc currDesc;
		bool found;
		int counter = 0;

		// Iterate through entries until we are back at the starting location
		// or we find a free frame
		while (!found && (counter < numBufs)) {

			advanceClock();

			// Get description of current frame
			currDesc = bufDescTable[clockHand];

			// If valid is not set, use current frame
			if (!currDesc.valid) {

				frame = currDesc.frameNo;
				found = true;
			}
			// If valid and refbit set, clear refbit and advance clock
			else if (currDesc.refbit) {

				currDesc.refbit = false;
			}
			// If frame is valid and not pinned, use it.
			else if (currDesc.pinCnt <= 0) {

				// If frame is dirty, write page to disk before using
				if (currDesc.dirty) {

					currDesc.file->writePage(bufPool[currDesc.frameNo]);
				}

				// Initialize frame for new data
				currDesc.Clear();

				// Remove frame from hash table
				hashTable->remove(currDesc.file, currDesc.pageNo);

				frame = currDesc.frameNo;
				found = true;
			}
			counter++;
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
			// GEt desired page in hash table
			BufDesc frame = bufDescTable[frameNo];
			//  Set refbit to true
			frame.refbit = true;
			// Inc pint count
			frame.pinCnt++;
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
		try {
			// Lookup hash
			hashTable->lookup(file, pageNo, frame_id);
			// find frame in table
			BufDesc frame = bufDescTable[frame_id];

			// throw page not pinned if not pinned
			if (frame.pinCnt <= 0) {
				throw PageNotPinnedException(file->filename(), pageNo, frame_id);
			}

			// udpate dirty value
			if (dirty) {
				frame.dirty = true;
			}

			// decrement from being unpinned
			frame.pinCnt--;

		}
		catch (HashNotFoundException h) {
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
