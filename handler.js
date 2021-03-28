'use strict';
const bent = require('bent');
const get = bent('string')
const AWS = require('aws-sdk');
const processData = require('./consolidate');
const {gzip} = require('node-gzip');
const population = require('population')
const buckets = ['website-jalrrb', 'website-1l4vj5'];
const jsuPath = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/";
const owidPath = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/";
const csv = {
    globCases: [`${jsuPath}time_series_covid19_confirmed_global.csv`, "cases.csv"],
    globDeaths: [`${jsuPath}time_series_covid19_deaths_global.csv`, "deaths.csv"],
    usCases: [`${jsuPath}time_series_covid19_confirmed_US.csv`, "cases_US.csv"],
    usDeaths: [`${jsuPath}time_series_covid19_deaths_US.csv`, "deaths_US.csv"],
    //usTests: [`https://api.covidtracking.com/v1/states/daily.csv`, "tests_US.csv"],
    usTestsCDC: [`https://beta.healthdata.gov/api/views/j8mb-icvb/rows.csv?accessType=DOWNLOAD&bom=true&format=true`, "tests_US.csv"],
    globTests: [`${owidPath}testing/covid-testing-all-observations.csv`, "tests.csv"],
    vaccinations: [`${owidPath}vaccinations/vaccinations.csv`, "vaccinations.csv"],
    usVaccinations: [`${owidPath}vaccinations/us_state_vaccinations.csv`, "vaccinations_us.csv"]
}

module.exports.processCSV = async (event,context,callback) => {
    try {
        for (let prop in csv) {
            const data = await get(csv[prop][0]);
            await writeFile(data, csv[prop][1], );
            csv[prop] = data;
        }

        const [cases, pop, dates] = await processData(csv, population);
        const data = JSON.stringify({dates: dates, data: cases});
          await writeFile(data , "jhu.js", "application/javascript");
        return {
            statusCode: 200,
            body: "OK"
        }
    } catch (e) {
        return {
            statusCode: 500,
            body: "Processing Error" + e.toString() + e.stack
        }
    }
    async function writeFile(data, key, type) {
        const s3 = new AWS.S3();
        for (let ix = 0; ix < buckets.length; ++ix) {
            const bucket = buckets[ix];
            const destparams = {
                Bucket: bucket,
                Key: key,
                Expires: new Date(new Date().getTime() + 1 * 60 * 1000),
                Body: (new Date()).toString() + "\n" + data,
                ContentType: type
            };
            await s3.putObject(destparams).promise();
        }
    }
}

