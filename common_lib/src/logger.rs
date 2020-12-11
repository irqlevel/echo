pub mod logger {

use log::{Record, Level, Metadata};

pub struct SimpleLogger;

impl SimpleLogger {
    fn level_to_systemdlevel(level: Level) -> u32 {
        /*
            <7>This is a DEBUG level message
            <6>This is an INFO level message
            <5>This is a NOTICE level message
            <4>This is a WARNING level message
            <3>This is an ERR level message
            <2>This is a CRIT level message
            <1>This is an ALERT level message
            <0>This is an EMERG level message
        */
        match level {
            Level::Debug => {
                7
            }
            Level::Error => {
                3
            }
            Level::Info => {
                6
            }
            Level::Trace => {
                7
            }
            Level::Warn => {
                4
            }
        }
    }
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }


    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("<{}>{}", SimpleLogger::level_to_systemdlevel(record.level()), record.args());
        }
    }

    fn flush(&self) {}
}

}