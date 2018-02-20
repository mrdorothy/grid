/****************************************** GLOBAL CONSTANTS *************************************/

const reset = "\x1b[0m"

const bright = "\x1b[1m"
const dim = "\x1b[2m"
const underscore = "\x1b[4m"
const blink = "\x1b[5m"
const reverse = "\x1b[7m"
const hidden = "\x1b[8m"

const black = "\x1b[30m"
const red = "\x1b[31m"
const green = "\x1b[32m"
const yellow = "\x1b[33m"
const blue = "\x1b[34m"
const magenta = "\x1b[35m"
const cyan = "\x1b[36m"
const white = "\x1b[37m"

const background_black = "\x1b[40m"
const background_red = "\x1b[41m"
const background_green = "\x1b[42m"
const background_yellow = "\x1b[43m"
const background_blue = "\x1b[44m"
const background_magenta = "\x1b[45m"
const background_cyan = "\x1b[46m"
const background_white = "\x1b[47m"

/******************************************** MAIN SCRIPT ****************************************/

// This is an improved logging function which is optimised to automate much of what
// i use console.log() for. All three arguments are optional. 
//
// If no message is provided then the function logs 'Taco'. 
//
// If a 'message_type' is provided as one of: 
//
// 	wrn, warning, w, err, error, e, suc, success, s, inf, info, i
//
// then the log will be prefaced with a log type (error, success, warning or info), otherwise
// the log will be prefaced with 'Debug Log'. 
//
// Special arguments can be provided which are colored blue. These use pgp syntax where
// by a placeholder such as $1 is left in the message then an array of arguments is provided
// as the third input to the function.
//
// For example:
//
// log('Something went wrong with $1','err',['David'])
//
// which outputs:
//
// 'Debug Log: Something went wrong with David'

module.exports = function(message, message_type, arguments_array) {
    message = message || 'Taco'
    message_type = message_type || ''
    arguments_array = arguments_array || []
    var output = ''
    var arguments_color = blue

    if (message_type.toLowerCase() == 'wrn' || message_type.toLowerCase() == 'warning' || message_type.toLowerCase() == 'w') {
        output += background_yellow + black + 'Warning:' + reset + ' '
    } else if (message_type.toLowerCase() == 'err' || message_type.toLowerCase() == 'error' || message_type.toLowerCase() == 'e') {
        output += background_black + red + 'Error:' + reset + ' '
    } else if (message_type.toLowerCase() == 'suc' || message_type.toLowerCase() == 'success' || message_type.toLowerCase() == 's') {
        output += background_green + white + 'Success:' + reset + ' '
    } else if (message_type.toLowerCase() == 'inf' || message_type.toLowerCase() == 'info' || message_type.toLowerCase() == 'i') {
        output += background_white + blue + 'Info:' + reset + ' '
    } else {
        output += background_blue + white + 'Debug:' + reset + ' '
    }
    if (arguments_array.length==0) {
        if (typeof(message)=='object') {
            console.log(output + reverse + 'object' + reset)
            console.log(message)
        } else {
            console.log(output + message)
        }
        return
    } else {
        for (char = 0; char < message.length - 1; char++) {
            if (message[char] == '$' && !isNaN(message[char + 1])) {
                if ((message[char + 1] - 1) < arguments_array.length) {
                    output += arguments_color + JSON.stringify(arguments_array[message[char + 1] - 1]) + reset
                } else {
                    output += arguments_color + '{missing arguement: ' + (message[char + 1] - 1) + '}' + reset
                }
            } else if (!(message[char - 1] == '$' && !isNaN(message[char]))) {
                output += message[char]
            }
        }
        if (!(message[message.length - 2] == '$' && !isNaN(message[message.length - 1])) || message.length < 2) {
            output += message[message.length - 1]
        }
    }
    console.log(output)
}
