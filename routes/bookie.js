// Requirements

var http = require(`http`)
var fs = require(`fs`)
var log = require(`../helpers/log.js`)
var requester = require(`request`)
var cheerio = require(`cheerio`)
var unzipper = require(`unzipper`)
var csv = require(`csvtojson`)
var db = require(`../db.js`);
var async = require(`async`)
var moment = require(`moment`)
var pgp = require('pg-promise')();

// Constants

const check_point = `--------> time taken since last checkpoint`
const staging = `./file_staging/`
const file_check_attempts = 10
const scada_keyword = `PUBLIC_DISPATCHSCADA_`

const maximum_files_for_testing = 3

// Kick off

exports.kickoff = () => {
    book_keeping_archive_generation()
}

// Collect current files from nemweb

book_keeping_current_generation = () => {

    // Initial set up

    var start_time = process_begin(`current book keeping`)
    var url = `http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/`
    var minutes_between_runs = 5
    var temp_sub_staging = ``

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
            get_links(url, scada_keyword, links => {
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
        },
        create_sub_staging: call_back => {
            create_temp_staging(1, (sub_stage_name) => {
                temp_sub_staging = sub_stage_name
                call_back(null, sub_stage_name)
            })
        }
    }, (err, results) => {
        if (results.previous_stats == `error` || results.links == `error` || results.current_timestamps == `error` || results.create_sub_staging == `error`) {
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
            download_array(required_files, temp_sub_staging, () => {

                checkpoint(`${required_files.length} files downloaded`)

                // Sort out files

                var zips_to_extract = []
                var csvs_to_upload = []
                for (file of required_files) {
                    var short_file_name = trim_url_to_file(file)
                    zips_to_extract.push(short_file_name)
                    csvs_to_upload.push(short_file_name.substring(0, short_file_name.length - 3) + `csv`)
                }

                unzip_array(zips_to_extract, temp_sub_staging, () => {

                    async.map(csvs_to_upload, (csv_file, unzip_complete) => {
                        wait_for_file(csv_file, temp_sub_staging, () => {
                            unzip_complete(null)
                        }, () => {
                            log(`Attempted to find ${csv_file} ${file_check_attempts} times without any luck! Might want to look into this.`, `e`)
                            unzip_complete(null)
                        }, 1, file_check_attempts, 100)
                    }, () => {

                        checkpoint(`Unzipped ${zips_to_extract.length} files`)

                        // Parse to an array

                        parse_generation_data(csvs_to_upload, temp_sub_staging, (upload_array) => {

                            if (upload_array.length > 0) {
                                var insert_query = pgp.helpers.insert(upload_array, [`timestamp`, `id`, `output`], 'generation')
                            }

                            async.parallel({
                                upload_data: call_back => {
                                    if (upload_array.length > 0) {
                                        db.query(`${insert_query};`)
                                            .then(() => {
                                                call_back(null, 'success')
                                            })
                                            .catch(err => {
                                                log(`There was some issue querying the generation db whilst uploading the data.`, `e`)
                                                console.log(err)
                                                call_back(null, `error`)
                                            })
                                    } else {
                                        call_back(null, 'success')
                                    }
                                },
                                tidy_up_staging: call_back => {
                                    purge_sub_staging(temp_sub_staging, () => {
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

                                    log(`See you in ${minutes_between_runs} minutes`, 'i')

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

// Collect archive files from nemweb

book_keeping_archive_generation = () => {

    // Initial set up

    var start_time = process_begin(`archive book keeping`)
    var url = `http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/`
    var temp_sub_staging = ``

    // In parallel collect stats, get links, get current timestamps and how many dispatch windows we have

    async.parallel({
        links: call_back => {
            get_links(url, scada_keyword, links => {
                call_back(null, links)
            })
        },
        archive_timestamps: call_back => {
            timestamp_query = `
                SELECT 
                    datestamp, COUNT(datestamp) as num_data_points 
                FROM 
                    (SELECT 
                        DATE(timestamp) as datestamp 
                    FROM 
                        (SELECT 
                            DISTINCT timestamp 
                        FROM 
                            generation
                        ) as baa
                    ) as foo 
                GROUP BY 
                    datestamp 
                ORDER BY 
                    datestamp;`
            db.query(timestamp_query)
                .then(timestamps => {
                    call_back(null, timestamps)
                })
                .catch(err => {
                    log(`There was some issue querying the generation db whilst getting the current timestamps.`, `e`)
                    console.log(err)
                    call_back(null, `error`)
                })
        },
        unique_timestamps: call_back => {
            timestamp_query = `
                SELECT DISTINCT 
                    timestamp 
                FROM
                    generation;`
            db.query(timestamp_query)
                .then(unique_timestamps => {
                    call_back(null, unique_timestamps)
                })
                .catch(err => {
                    log(`There was some issue querying the generation db whilst getting the current unique timestamps.`, `e`)
                    console.log(err)
                    call_back(null, `error`)
                })
        },
        create_sub_staging: call_back => {
            create_temp_staging(1, (sub_stage_name) => {
                temp_sub_staging = sub_stage_name
                call_back(null, sub_stage_name)
            })
        }
    }, (err, results) => {
        if (results.links == `error` || results.archive_timestamps == `error` || results.unique_timestamps == `error` || results.create_sub_staging == `error`) {
            log(`There was a fatal error in the collection process`)
        } else {

            checkpoint(`completed initial queries`)

            // For archive files just check the dates vs. the filenames to see which ones we want.

            var required_files = []
            for (file of results.links) {
                var file_timestamp = file.substring(file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 21, file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 8 + 21)
                var already_collected = false
                for (var check_timestamp of results.archive_timestamps) {
                    if (moment(check_timestamp.timestamp).format(`YYYYMMDD`) == file_timestamp && check_timestamp.num_data_points > 287) {
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

            download_array(required_files, temp_sub_staging, () => {

                checkpoint(`${required_files.length} files downloaded`)

                // Sort out files

                var zips_to_extract = []
                for (file of required_files) {
                    var short_file_name = trim_url_to_file(file)
                    zips_to_extract.push(short_file_name)
                }

                // We now have a list of all archive days, now we want to extract each to its own sub stage and then in parrallel upload them

                upload_archive_timestamps = []

                async.map(zips_to_extract, (archive_zip, archive_date_complete_analysis) => {
                    var archive_zip_sub_stage
                    create_temp_staging(1, (sub_stage_name) => {
                        archive_zip_sub_stage = sub_stage_name
                        unzip_single(archive_zip, temp_sub_staging, archive_zip_sub_stage, () => {

                            // Now we need to get an array of dates and timestamps from the db

                            fs.readdir(staging + archive_zip_sub_stage, (err, files) => {
                                upload_archive_timestamps.push ({ directory: archive_zip_sub_stage, files: [], csvs: [] })
                                files.forEach(file => {
                                    var temp_timestamp = file.substring(file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 21, file.indexOf(`PUBLIC_DISPATCHSCADA_`) + 12 + 21)
                                    var already_collected = false
                                    for (var check_timestamp of results.unique_timestamps) {
                                        if (moment(check_timestamp.timestamp).format(`YYYYMMDDHHmm`) == temp_timestamp) {
                                            already_collected = true
                                            break
                                        }
                                    }
                                    if (!already_collected) {
                                        upload_archive_timestamps[upload_archive_timestamps.length-1]['files'].push(file)
                                        upload_archive_timestamps[upload_archive_timestamps.length-1]['csvs'].push(file.substring(0, file.length - 3) + `csv`)
                                    }
                                })
                                archive_date_complete_analysis(null)
                            })
                        })
                    })
                }, () => {

                    checkpoint(`${upload_archive_timestamps.length} archive files analysed`)

                    // Now we need to run the very standard unzip all, upload all csvs, purge all

                    async.map(upload_archive_timestamps, (archive_date, archive_date_complete) => {

                        unzip_array(archive_date.files, archive_date.directory, () => {
                            //parse csvs

                            archive_date_complete(null)
                        })

                    }, () => {

                        log('finished','s')

                    })
                })


            })

        }
    })
}


// Generic downloading function

download = (address, sub_staging, success_call_back, error_call_back) => {

    var file = fs.createWriteStream(staging + sub_staging)

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

download_array = (required_files, sub_staging, call_back) => {
    process.stdout.write(`0 of ${required_files.length} files downloaded...`)
    var downloaded_files = 0

    async.map(required_files, (file, download_complete) => {
        file_name = sub_staging + trim_url_to_file(file)
        download(file, file_name, () => {
            wait_for_file(file_name, sub_staging, () => {

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

unzip_array = (required_files, sub_staging, call_back) => {
    process.stdout.write(`0 of ${required_files.length} files unzipped...`)
    var unzipped_files = 0

    async.map(required_files, (file, unzip_complete) => {
        fs.createReadStream(staging + sub_staging + file).pipe(unzipper.Extract({ path: staging + sub_staging })).on('close', () => { // Unzip the file
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

// Single unzip 

unzip_single = (file, sub_staging, destination, call_back) => {

    fs.createReadStream(staging + sub_staging + file).pipe(unzipper.Extract({ path: staging + destination })).on('close', () => { // Unzip the file
        call_back()
    })

}

// Test a particular file

test_particular_file = (file_to_test) => {

    var column_headings = [`i`, `d`, `u`, `n`, `timestamp`, `id`, `output`]

    csv({ noheader: false, headers: column_headings }).fromFile(staging + file_to_test)
        .then((data) => {
            data.forEach(point => {
                log(point.id + ',' + point.output)
            })
        })

}

// Parse generation data file

parse_generation_data = (required_files, sub_staging, call_back) => {

    var column_headings = [`i`, `d`, `u`, `n`, `timestamp`, `id`, `output`]
    var upload_array = []
    var parsed_files = 0

    async.map(required_files, (file, parse_complete) => {
        log(staging + sub_staging + file)
        csv({ noheader: false, headers: column_headings }).fromFile(staging + sub_staging + file).then((data) => {
            var temp_data_array = []
            data.forEach(point => {
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

        })
    }, () => {

        process.stdout.write(`\n\n`)
        call_back(upload_array)

    })
}

// Check if a file exists every wait_time and then proceed with success_call_back if it does

wait_for_file = (file_name, sub_staging, success_call_back, error_call_back, current_attempt, attempts, wait_time) => {
    fs.access(staging + sub_staging, file_name, (err) => {
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
    file_name = file_name.substring(last_slash + 1, file_name.length)
    return file_name
}

// Creates a temporary staging folder within the staging folder

create_temp_staging = (current_stage_attempt, call_back) => {

    var max_folder_attempts = 1000
    var folder_name = current_stage_attempt

    if (folder_name > max_folder_attempts) {
        log(`Failed to create directory ${staging}${folder_name} ${err}`, 'e');
        call_back(`error`)
    } else {
        fs.mkdir(staging + folder_name, (err) => {
            if (err) {
                folder_name += 1
                create_temp_staging(folder_name, call_back)
            } else {
                call_back(folder_name + '/')
            }
        })
    }

}

purge_sub_staging = (sub_staging, call_back) => {
    fs.readdir(staging + sub_staging, (err, deleting_files) => {
        if (err) {
            log('There was some error deleting files from staging: ' + err, 'e')
        } else {
            for (file of deleting_files) {
                fs.unlink(staging + sub_staging + file)
            }
            fs.rmdir(staging + sub_staging)
            call_back()
        }
    })
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