const fs = require('fs/promises');
const chokidar = require('chokidar');
const logger = require('loglevel');

const sleep = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
};
const round = (num) => Math.round((num + Number.EPSILON) * 100) / 100;
const regExp = new RegExp(/\d{4}(_\d{2}){4}\.txt$/);
const SUFFIX = '.inProgress';
logger.setLevel('info');

class ProcessFiles {
    /**
     *
     * @param {number} secondsInMin The number of seconds in a minute. For debugging purposes, as suggested.
     * @param {number} keep The number of files to keep the processing times for and log after.
     */
    constructor(secondsInMin = 60, keep = 5) {
        this.secondsInMin = secondsInMin;
        this.keep = keep;
        this.processingQueue = [];  // A queue to keep the added filenames in
        this.processingTimes = [];  // A queue to keep the last X ('keep' variable above) processing time in
        this.folderName = '';
        this.processingCounter = 0;
        this.totalProcessingTime = 0;
    }

    async setup() {
        /**
         * Checks for the existence of a supplied path command line parameter.
         * Handles error handling if the parameter value doesn't correspond to an existing folder.
         * Assuming that the folder location is relative to the script's (this file) path.
         */
        const cmdLine = process.argv.slice(2);
        if (cmdLine.length === 0 || !cmdLine[0]) {
            logger.error('No folder specified. Folder must be specified for script to operate. Exiting.');
            process.exit(1);
        }
        this.folderName = cmdLine[0];
        try {
            const folderStat = await fs.stat(this.folderName);
            if (!folderStat.isDirectory()) {
                logger.error(`${this.folderName} isn't a folder. Exiting.`);
                process.exit(2);
            }
        } catch (e) {
            logger.error(`Specified folder doesn't exist: "${this.folderName}". Exiting.`);
            process.exit(3);
        }
    }

    listenForIncoming() {
        /**
         * Uses 'chokidar' to listen on a specific file system folder for new files.
         * Assuming that if the file name matches a specific regular expression, then it must be the type of file we
         * need to "process".
         * Assuming that the challenge wasn't to set up an S3 bucket, but rather to simulate it locally.
         */
        const watcher = chokidar.watch(this.folderName);
        watcher.on('add', path => {
            logger.debug('Found a new file', path);
            const match = path.match(regExp);
            if (match) {
                logger.debug('Found matched file. Adding to queue.');
                this.enqueue(path);
            }
        });
    }

    enqueue(fileName) {
        /**
         * Appends a file to the end of the queue.
         * @param {string} fileName The path of the filename to add to the queue.
         */
        this.processingQueue.push(fileName);
    }

    dequeue() {
        /**
         * If the processing queue contains items, fetches and returns the oldest item. FIFO.
         * @returns {string} The first queue item.
         */
        return this.processingQueue.shift();
    }

    async rename(fileName) {
        /**
         * If the file exists and is ready, renames the file (adds a suffix) to indicate that a worker
         * is working on it and that it should not be processed by the other worker as well.
         * @param {string} fileName The filename to rename.
         */
        try {
            const stats = await fs.stat(fileName);
            if (stats.size === 0) {  // Hack for when the file isn't ready. Not expected to happen on S3.
                this.enqueue(fileName);
                return false;
            }
        } catch (e) {
            // Using 'debug' level instead of 'error' because we expect this to happen on the other worker (by design).
            logger.debug(`Failed getting stats for ${fileName}`);
            return false;
        }
        try {
            await fs.rename(fileName, fileName + SUFFIX);
            logger.debug(`Successfully renamed ${fileName}`);
            return true;
        } catch (e) {
            logger.error(`Unable to rename ${fileName} ${e}`);
            return false;
        }
    }

    async delete(fileName) {
        /**
         * Deletes the file from the file system.
         * Assuming it's not needed after we "processed" it.
         * @param {string} fileName The filename to remove from the file system.
         */
        try {
            await fs.unlink(fileName + SUFFIX);
            logger.debug(`Successfully deleted ${fileName}${SUFFIX}`);
        } catch (e) {
            logger.warn(`Unable to delete ${fileName}${SUFFIX} ${e}`);
        }
    }

    async processFile(fileName) {
        /**
         * Read the file and "processes" it by sleeping the amount of time specified in its contents.
         * @param {string} fileName The filename to process.
         * @returns {number} The amount of time it took to process.
         */
        try {
            const body = await fs.readFile(fileName + SUFFIX, {encoding: 'utf-8', flag: 'r'});
            const processingTime = parseFloat(body);
            logger.debug(`Processing ${fileName}${SUFFIX} Waiting for ${body} minutes.`);
            await sleep(processingTime * this.secondsInMin * 1000);
            this.processingCounter++;
            return processingTime;
        } catch (e) {
            logger.error(`Unable to read ${fileName}${SUFFIX} ${e}`);
            return 0;
        }
    }

    logging(processingTime) {
        /**
         * Sums the total processing time and maintains the last X (keep) values in the processingTimes queue.
         * @param {number} processingTime The number to log and add to the total.
         */
        this.totalProcessingTime = this.totalProcessingTime + processingTime;
        if (this.processingTimes.length <= this.keep) {
            this.processingTimes.push(processingTime);
        }
        if (this.processingTimes.length > this.keep) {
            this.processingTimes.shift();
        }
    }

    calcRollingSquaredDifferences() {
        /**
         * Calculates the rolling squared differences of time that worker spent processing. If, for a
         * specific worker, the historical time spent processing files was equal to 5,6,7,8 and the
         * next (5th) file coming in was 9 then this value would be: (5-9)^2 + (6-9)^2 + (7-9)^2
         * + (8-9)^2 + (9-9)^2= 30. In other words, you take the current processing time and
         * subtract it from the historical processing times, square that number and then sum it.
         * @return {number} The rolling squared differences
         */
        let sum = 0;
        let last = this.processingTimes.length - 1;
        for (let i = 0; i <= last; i++) {
            sum = sum + Math.pow(this.processingTimes[i] - this.processingTimes[last], 2);
        }
        return sum;
    }

    logFormat() {
        /**
         * Assuming rounding is OK.
         * @returns {string} The desired log line format.
         */
        return `Files processed: ${this.processingCounter}, Total processing time: ${round(this.totalProcessingTime)} Sum of Squares: ${round(this.calcRollingSquaredDifferences())}`;
    }
}

const main = async () => {
    /**
     * The Main function of the script:
     * 1. Instantiates the ProcessFiles class
     * 2. Processes the queue forever, file by file
     * 3. Logs
     * 4. Deletes
     */
    const pFiles = new ProcessFiles(0.1);
    await pFiles.setup();
    pFiles.listenForIncoming();
    while (true) {
        await sleep(1000 * Math.random());  // Sleeping for a random number to minimize race condition with other worker
        const fName = pFiles.dequeue();
        if (!fName) continue;  // If no file in the queue skip it's processing instructions
        logger.debug(`Dequeued ${fName} Queue size: ${pFiles.processingQueue.length}`);
        if ((await pFiles.rename(fName)) === false) continue;  // If file rename failed, don't bother processing
        const pTime = await pFiles.processFile(fName);
        pFiles.logging(pTime);
        pFiles.delete(fName);  // No need to "await" for the deletion
        if (pFiles.processingCounter % pFiles.keep === 0) {  // Log every X (keep) number of files
            logger.info(pFiles.logFormat());
        }
    }
};

main().catch(err => logger.error(err));
