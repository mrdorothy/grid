// Requirements

var http = require(`http`)
var fs = require(`fs`)
var log = require(`../helpers/log.js`)
var requester = require(`request`)
var cheerio = require(`cheerio`)
var unzipper = require(`unzipper`)
var csv = require(`csv-to-array`)
var db = require(`../db.js`);
var async = require(`async`)
var moment = require(`moment`)
var pgp = require('pg-promise')();

// Constants

const check_point = `--------> time taken since last checkpoint`
const staging = `./file_staging/`
const file_check_attempts = 10

// Kick off

exports.kickoff = () => {
    book_keeping()
}

// Collect current files from nemweb

book_keeping = () => {

    // Initial set up

    var start_time = process_begin(`book keeping`)
    var maximum_files_for_testing = 5000
    var url = `http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/`
    var key_word = `PUBLIC_DISPATCHSCADA_`
    var minutes_between_runs = 5

    // In parallel collect stats, get links, get current timestamps

    async.parallel({
        previous_stats: call_back => {
            var table_name = `generation`
            var column_name = `timestamp`
            db_length(table_name, column_name, previous_number_of_points => {
                call_back(null, previous_number_of_points)
            })
        },
        links: call_back => {
            get_links(url, key_word, links => {
                call_back(null, links)
            })
        },
        current_timestamps: call_back => {
            timestamp_query = `
                SELECT DISTINCT 
                    timestamp 
                FROM
                    generation;`
            db.query(timestamp_query)
                .then(timestamps => {
                    call_back(null, timestamps)
                })
                .catch(err => {
                    log(`There was some issue querying the generation db whilst getting the current timestamps.`, `e`)
                    console.log(err)
                    call_back(null, `error`)
                })
        }
    }, (err, results) => {
        if (results.previous_stats == `error` || results.links == `error` || results.current_timestamps == `error`) {
            log(`There was a fatal error in the collection process`)
        } else {

            checkpoint(`completed initial queries`)

            // This part is unique to this process, check the timestamps vs. the filenames to see which ones we want.

            var required_files = []
            for (file of results.links) {
                var file_timestamp = file.substring(file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 21, file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 12 + 21)
                var already_collected = false
                for (var check_timestamp of results.current_timestamps) {
                    if (moment(check_timestamp.timestamp).format(`YYYYMMDDHHmm`) == file_timestamp) {
                        already_collected = true
                        break
                    }
                }
                if (!already_collected && required_files.length < maximum_files_for_testing) {
                    required_files.push(`http://www.nemweb.com.au${file}`)
                }
            }

            checkpoint(`cross checked available files against current database`)

            // Download all the files

            download_array(required_files, () => {

                checkpoint(`${required_files.length} files downloaded`)

                // Sort out files

                var zips_to_extract = []
                var csvs_to_upload = []
                for (file of required_files) {
                    var short_file_name = trim_url_to_file(file)
                    zips_to_extract.push(short_file_name)
                    csvs_to_upload.push(short_file_name.substring(0, short_file_name.length - 3) + `csv`)
                }

                unzip_array(zips_to_extract, () => {

                    async.map(csvs_to_upload, (csv_file, unzip_complete) => {
                        wait_for_file(csv_file, () => {
                            unzip_complete(null)
                        }, () => {
                            log(`Attempted to find ${csv_file} ${file_check_attempts} times without any luck! Might want to look into this.`, `e`)
                            unzip_complete(null)
                        }, 1, file_check_attempts, 100)
                    }, () => {

                        checkpoint(`Unzipped ${zips_to_extract.length} files`)

                        // Parse to an array

                        parse_generation_data(csvs_to_upload, (upload_array) => {

                            var insert_query = pgp.helpers.insert(upload_array, [`timestamp`, `id`, `output`], 'generation')

                            async.parallel({
                                upload_data: call_back => {
                                    db.query(`${insert_query};`)
                                        .then(() => {
                                            call_back(null, 'success')
                                        })
                                        .catch( err => {
                                            log(`There was some issue querying the generation db whilst uploading the data.`, `e`)
                                            console.log(err)
                                            call_back(null, `error`)
                                        })
                                },
                                tidy_up_staging: call_back => {
                                    get_links(url, key_word, links => {
                                        // Need to add delete function here
                                        call_back(null, 'success')
                                    })
                                }
                            }, (err) => {
                                var table_name = `generation`
                                var column_name = `timestamp`
                                db_length(table_name, column_name, current_number_of_points => {

                                    checkpoint(`uploaded data and cleaned up staging`)
                                    log(`DB stats -----> PREVIOUS NUMBER OF DATA POINTS: $1`, `i`, [results.previous_stats.total_points])
                                    log(`DB stats -----> CURRENT NUMBER OF DATA POINTS:  $1`, `i`, [current_number_of_points.total_points])
                                    log(`DB stats -----> REQUIRED TO BE ADDED:           $1`, `i`, [upload_array.length])
                                    log(`DB stats -----> ACTUAL ADDED:                   $1`, `i`, [current_number_of_points.total_points - results.previous_stats.total_points])
                                    log(`DB stats -----> CURRENT DISPATCH INTERVALS:     $1`, `i`, [current_number_of_points.total_unique_points])
                                    process_end('book_keeping', start_time)

                                    log(`See you in ${minutes_between_runs} minutes`,'i')

                                    // Now sleep for 5 minutes

                                    setTimeout(() => {
                                        book_keeping()
                                    }, minutes_between_runs * 60000)

                                })
                            })
                        })
                    })
                })
            })
        }
    })
}

/*// Initial set up

console.time(`-> time`)
var url = `http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/`
var minutes_between_runs = 5
var collect_data = []
var required_files = []
var start_time = moment()
var max_run_index = 5000 // Temp max files testing smaller batches
console.log(`\n------------------------------------------------------------------------------------\n`)
log(`|| ${start_time.format(`DD MMM YY HH:mm:ss`)} | Commencing current generation data extraction ||\n`, `i`)
checkpoint()
log(`Collecting previous list of timestamps...`, `i`)

// Query for existing timestamps 

db.multi(`SELECT DISTINCT 
                timestamp 
            FROM
                generation;
            SELECT 
                COUNT(timestamp) as previous_data_count
            FROM
                generation;`)
    .then(timestamps => {
        log(`1. Collected previous list of timestamps |\n`, `s`)
        checkpoint()
        log(`Checking nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/ for currently available files...`, `i`)

        // Get list of files to download

        requester(url, (err, resp, body) => {

            $ = cheerio.load(body)
            var links = $(`a`)
            var total_checked = 0

            $(links).each((i, link) => {
                var link_string = $(link).attr(`href`)
                if (link_string.indexOf(`PUBLIC_DISPATCHSCADA_`) != -1) {
                    total_checked += 1
                    var link_timestamp = link_string.substring(link_string.indexOf(`PUBLIC_DISPATCHSCADA_`) + 21, link_string.indexOf(`PUBLIC_DISPATCHSCADA_`) + 12 + 21)
                    var already_found = false
                    for (var timestamp of timestamps[0]) {
                        if (moment(timestamp.timestamp).format(`YYYYMMDDHHmm`) == link_timestamp) {
                            already_found = true
                            break
                        }
                    }
                    if (already_found == false) {
                        var file_name = link_string.substring(link_string.indexOf(`PUBLIC_DISPATCHSCADA_`), link_string.length)
                        var output_file = `./file_staging/${file_name}`
                        required_files.push({
                            link: link_string,
                            file_name: file_name,
                            output_file: output_file,
                            output_csv: `${output_file.slice(0, output_file.length - 4)}.csv`,
                            timestamp: link_timestamp
                        })
                    }
                }
            })

            log(`2. Collected list of available files from NEMweb nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/ |\n`, `s`)
            checkpoint()
            log(`${total_checked} files available, there are only ${required_files.length} which we don't have...`, `i`)
            log(`Downloading from nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/ into an INSERT query...\n`, `i`)

            // Download the files, extract them to CSV then store them in a concatenated string for inserting in the DB

            var file_check_attempts = 10

            var column_headings = [`i`, `d`, `u`, `n`, `timestamp`, `id`, `output`];
            var run_index = 0 // Temp counter for testing smaller batches
            total_required = required_files.length
            if (required_files.length > max_run_index) {
                log(`Max files is set to ${max_run_index}, which is less than the currently available files (${required_files.length}).\n`, `i`)
                total_required = max_run_index
            }

            if (required_files.length > 0) {
                process.stdout.write(`0 of ${total_required} files downloaded and parsed `)
                var downloaded_files = 0
                async.map(required_files, (file, file_complete) => {

                    if (run_index < max_run_index) {
                        run_index += 1

                        download(`http://www.nemweb.com.au${file.link}`, file.output_file, () => { // Download the file

                            fs.createReadStream(file.output_file).pipe(unzipper.Extract({ path: `./file_staging/` })).on('close', () => { // Unzip the file

                                wait_for_file(file.output_csv, () => {
                                    csv({ file: file.output_csv, columns: column_headings }, (err, array) => {
                                        if (err) {
                                            console.log(err)
                                            file_complete(null)
                                        } else {
                                            var temp_data_array = []
                                            array.forEach(point => {
                                                if (point.i == `D` && point.output != 0) {
                                                    var already_exists = false
                                                    for (row_1 = 0; row_1 < temp_data_array.length; row_1++) {
                                                        if (temp_data_array[row_1].timestamp == point.timestamp && temp_data_array[row_1].id == point.id) {
                                                            already_exists = true
                                                        }
                                                    }
                                                    if (already_exists == false) {
                                                        temp_data_array.push({
                                                            timestamp: point.timestamp,
                                                            id: point.id,
                                                            output: point.output
                                                        })
                                                        collect_data.push({
                                                            timestamp: point.timestamp,
                                                            id: point.id,
                                                            output: point.output
                                                        })

                                                    }
                                                }
                                            })
                                            downloaded_files += 1
                                            process.stdout.clearLine()
                                            process.stdout.cursorTo(0)
                                            process.stdout.write(`${downloaded_files} of ${total_required} files downloaded and parsed`)
                                            file_complete(null)
                                        }
                                    })

                                }, () => {

                                    log(`---> Attempted to find ${file.output_csv} ${file_check_attempts} times without any luck! 
                                        Might want to look into this. |`, `e`)
                                    file_complete(null)

                                }, 1, file_check_attempts, 100)
                            })
                        }, () => {
                            log(`There was some error downloading ${file.link}`, `e`)
                            file_complete(null)
                        })

                    } else {
                        file_complete(null) // If exceeded max file count for testing purposes
                    }

                }, () => {

                    for (punch = 0; punch < 20; punch++) {
                        process.stdout.write(` !`)
                    }
                    process.stdout.write(`\n\n`)

                    // Create the INSERT query

                    var insert_query = pgp.helpers.insert(collect_data, [`timestamp`, `id`, `output`], 'generation')

                    log(`3. Downloaded, unzipped and compiled all files into an INSERT query |\n`, `s`)
                    checkpoint()
                    log(`Now uploading to the 'generation' DB...`, `i`)

                    // Upload data to the database

                    db.multi(`${insert_query} ON CONFLICT DO NOTHING;
                        SELECT 
                            COUNT(timestamp) as num_datapoints 
                        FROM 
                            generation; 
                        SELECT 
                            COUNT(timestamp) as num_data_days 
                        FROM 
                            (SELECT DISTINCT 
                                DATE(timestamp) 
                            FROM 
                                generation) as timestamp;`)
                        .then((return_data) => {

                            log(`4. Data uploaded to the 'generation DB |\n`, `s`)
                            checkpoint()
                            log(`Sweeping out the staging area...`, 'i')

                            async.map(required_files, (file, delete_complete) => {
                                fs.unlink(file.output_file, () => {
                                    delete_complete(null)
                                })
                            }, () => {
                                async.map(required_files, (file, delete_complete) => {
                                    fs.unlink(file.output_csv, () => {
                                        delete_complete(null)
                                    })
                                }, () => {

                                    log(`5. .zip & .csv files deleted |\n`, `s`)
                                    checkpoint()
                                    console.log(``)
                                    log(`|| ${start_time.format(`DD MMM YY HH:mm:SS`)} | Data extraction complete for ${total_required} files in ${convert_ms(moment().diff(start_time))} ||\n`, `i`)
                                    log(`DB stats -----> PREVIOUS NUMBER OF DATA POINTS: $1`, `i`, [timestamps[1][0].previous_data_count])
                                    log(`DB stats -----> CURRENT NUMBER OF DATA POINTS:  $1`, `i`, [return_data[1][0].num_datapoints])
                                    log(`DB stats -----> REQUIRED TO BE ADDED:           $1`, `i`, [collect_data.length])
                                    log(`DB stats -----> ACTUAL ADDED:                   $1`, `i`, [return_data[1][0].num_datapoints - timestamps[1][0].previous_data_count])
                                    if ((return_data[1][0].num_datapoints - timestamps[1][0].previous_data_count - collect_data.length) != 0) {
                                        log(`Didn't upload all of the collected data!`, `e`)
                                    }
                                    log(`DB stats -----> CURRENT DAYS OF DATA:           $1\n`, `i`, [return_data[2][0].num_data_days])
                                    log(`See you in 5 minutes\n`, `i`)
                                    console.log(`------------------------------------------------------------------------------------`)

                                    // Now sleep for 5 minutes

                                    setTimeout(() => {
                                        book_keeping()
                                    }, minutes_between_runs * 60000)

                                })
                            })
                        })
                        .catch(err => {
                            log(`---> There was some issue uploading data to the 'generation' DB | Might want to look into this. |`, `e`)
                            console.log(err);
                            log(`|| There was some issue querying the generation DB. Process exiting at ${start_time.format(`DD MMM YY HH:mm:ss`)}. Total time taken was ${convert_ms(moment().diff(start_time))} ||\n`, `i`)
                            log(`See you in 5 minutes\n`, `i`)
                            console.log(`------------------------------------------------------------------------------------`)
                        })
                })
            } else {
                log(`---> There were no files to be collected | Might want to look into this. |`, `e`)
                log(`|| There were no files to be collected. Process exiting at ${start_time.format(`DD MMM YY HH:mm:ss`)}. Total time taken was ${convert_ms(moment().diff(start_time))} ||\n`, `e`)
                log(`See you in 5 minutes\n`, `i`)
                console.log(`------------------------------------------------------------------------------------`)
            }
        })
    })
    .catch(err => {
        log(`---> There was some issue querying the generation DB | Might want to look into this. |`, `e`)
        console.log(err);
        log(`|| There was some issue querying the generation DB. Process exiting at ${start_time.format(`DD MMM YY HH:mm:ss`)} Total time taken was ${convert_ms(moment().diff(start_time))} ||\n`, `e`)
        log(`See you in 5 minutes\n`, `i`)
        console.log(`------------------------------------------------------------------------------------`)
    });*/


// Generic downloading function

download = (address, destination, success_call_back, error_call_back) => {

    var file = fs.createWriteStream(staging + destination)

    var request = http.get(address, response => {
        if (response.statusCode != 400 && response.statusCode != 200) {

            log(`---> Couldn't find file at ${address} | Might want to look into this. |`, `e`)
            error_call_back()

        } else {

            stream = response.pipe(file).on(`finish`, () => {
                success_call_back()
            })

        }
    })
}

// Batch download 

download_array = (required_files, call_back) => {
    process.stdout.write(`0 of ${required_files.length} files downloaded...`)
    var downloaded_files = 0

    async.map(required_files, (file, download_complete) => {
        file_name = trim_url_to_file(file)
        download(file, file_name, () => {
            wait_for_file(file_name, () => {

                downloaded_files += 1
                process.stdout.clearLine()
                process.stdout.cursorTo(0)
                process.stdout.write(`${downloaded_files} of ${required_files.length} files downloaded...`)
                download_complete(null)

            }, () => {

                log(`Attempted to find ${file} ${file_check_attempts} times without any luck! Might want to look into this.`, `e`)
                download_complete(null)

            }, 1, file_check_attempts, 100)
        }, () => {
            log(`There was some error downloading ${file}`, `e`)
            download_complete(null)
        })
    }, () => {
        process.stdout.write(`\n\n`)
        call_back()
    })
}

// Batch unzip 

unzip_array = (required_files, call_back) => {
    process.stdout.write(`0 of ${required_files.length} files unzipped...`)
    var unzipped_files = 0

    async.map(required_files, (file, unzip_complete) => {
        fs.createReadStream(staging + file).pipe(unzipper.Extract({ path: staging })).on('close', () => { // Unzip the file
            unzipped_files += 1
            process.stdout.clearLine()
            process.stdout.cursorTo(0)
            process.stdout.write(`${unzipped_files} of ${required_files.length} files unzipped...`)
            unzip_complete(null)
        })
    }, () => {
        process.stdout.write(`\n\n`)
        call_back()
    })
}

// Parse generation data file

parse_generation_data = (required_files, call_back) => {

    var column_headings = [`i`, `d`, `u`, `n`, `timestamp`, `id`, `output`]
    var upload_array = []
    var parsed_files = 0

    async.map(required_files, (file, parse_complete) => {
        csv({ file: staging + file, columns: column_headings }, (err, array) => {
            if (err) {
                console.log(err)
                parse_complete(null)
            } else {
                var temp_data_array = []
                array.forEach(point => {
                    if (point.i == `D` && point.output != 0) {
                        var already_exists = false
                        for (row_1 = 0; row_1 < temp_data_array.length; row_1++) {
                            if (temp_data_array[row_1].timestamp == point.timestamp && temp_data_array[row_1].id == point.id) {
                                already_exists = true
                            }
                        }
                        if (!already_exists) {
                            temp_data_array.push({
                                timestamp: point.timestamp,
                                id: point.id,
                                output: point.output
                            })
                            upload_array.push({
                                timestamp: point.timestamp,
                                id: point.id,
                                output: point.output
                            })

                        }
                    }
                })

                parsed_files += 1
                process.stdout.clearLine()
                process.stdout.cursorTo(0)
                process.stdout.write(`${parsed_files} of ${required_files.length} files parsed...`)
                parse_complete(null)

            }
        })
    }, () => {

        process.stdout.write(`\n\n`)
        call_back(upload_array)

    })
}

// Check if a file exists every wait_time and then proceed with success_call_back if it does

wait_for_file = (file_name, success_call_back, error_call_back, current_attempt, attempts, wait_time) => {
    fs.access(staging + file_name, (err) => {
        if (err) {
            if (current_attempt < attempts) {
                setTimeout(() => {
                    wait_for_file(file_name, success_call_back, error_call_back, current_attempt + 1, attempts, wait_time)
                }, 100)
            } else {
                error_call_back()
            }
        } else {
            success_call_back()
        }
    })
}

// Convert milliseconds into mintes & seconds, just logging sugar

convert_ms = ms => {
    hours = Math.floor(ms / 3600000),
        minutes = Math.floor((ms % 3600000) / 60000),
        seconds = Math.floor(((ms % 360000) % 60000) / 1000)
    if (hours > 0) {
        return `${hours}:${minutes}:${seconds}`
    } else {
        return `${minutes} minutes & ${seconds} seconds`
    }
}

// Work out how many points are in a database

db_length = (table, column, call_back) => {
    db.multi(`SELECT COUNT($1) as total_points
                FROM
                    ${table};
                SELECT
                    COUNT(foo) as total_unique_points
                FROM (SELECT DISTINCT
                        $1 as foo
                    FROM
                        ${table}) as bah;`, [column])
        .then(data_stats => {
            return_object = {
                total_points: data_stats[0][0].total_points,
                total_unique_points: data_stats[1][0].total_unique_points
            }
            call_back(return_object)
        })
        .catch(err => {
            log(`There was some issue querying the ${table} db whilst getting the stats.`, `e`)
            console.log(err)
            call_back(`error`)
        })
}

// Get an array of the links on a page

get_links = (url, key_word, call_back) => {
    var return_array = []
    requester(url, (err, resp, body) => {
        if (err) {
            log(`Some error occured whilst collecting links from ${url}`, `e`)
            console.log(err)
            call_back(`error`)
        }
        $ = cheerio.load(body)
        var links = $(`a`)
        $(links).each((i, link) => {
            if ($(link).attr(`href`).indexOf(key_word) != -1) {
                return_array.push($(link).attr(`href`))
            }
        })
        call_back(return_array)
    })
}

// Small function for beginning and end of a process

process_begin = process_name => {
    console.time(check_point)
    start_time = moment()
    console.log(`\n------------------------------------------------------------------------------------\n`)
    log(`|| ${start_time.format(`DD MMM YY HH:mm:ss`)} | Commencing ${process_name.toUpperCase()} ||`, `i`)
    console.log(`\n------------------------------------------------------------------------------------\n`)
    return start_time
}

process_end = (process_name, start_time) => {
    console.log(`\n------------------------------------------------------------------------------------\n`)
    log(`|| Finished ${process_name.toUpperCase()} | Time Taken: ${convert_ms(moment().diff(start_time))} ||`, `i`)
    console.log(`\n------------------------------------------------------------------------------------\n`)
}

// Gets rid of the website from a file download location

trim_url_to_file = file_name => {
    var last_slash = 0
    for (char = 0; char < file_name.length; char++) {
        if (file_name.substring(char, char + 1) == '/') {
            last_slash = char
        }
    }
    file_name = file_name.substring(last_slash+1, file_name.length)
    return file_name
}

// Small function for logging how long something takes, call console.timeEnd('-> time') first, then checkpoint() for each lap

checkpoint = message => {
    if (message) {
        console.log(`\x1b[42m Check Point: \x1b[0m ${message.toUpperCase()}`)
    } else {
        console.log(`\x1b[42m Check Point \x1b[0m`)
    }
    console.timeEnd(check_point)
    console.time(check_point)
    console.log(``)
}