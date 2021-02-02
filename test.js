const fs = require('fs').promises;
const {gzip} = require('node-gzip');
const processData = require('./consolidate');

doIt();

async function doIt () {
    const csv = await readFiles();
    const [cases, population, asof] = await processData(csv);
    console.log(JSON.stringify(cases["Total"]));
    console.log(JSON.stringify(cases["Georgia (Sakartvelo)"]));
    console.log(JSON.stringify(cases["Georgia"]));
    console.log(JSON.stringify(cases["Quebec, Canada"]));
    console.log(JSON.stringify(cases["Queensland, Australia"]));
    console.log(JSON.stringify(cases["Cayman Islands"]));
    console.log(JSON.stringify(cases["China"]));
    console.log(JSON.stringify(cases["South Korea"]));
    console.log(JSON.stringify(cases["Nassau, New York"]));
    console.log(JSON.stringify(cases["New York"]));
    console.log(JSON.stringify(cases["California"]));
    console.log(JSON.stringify(cases["United States"]));
    console.log(JSON.stringify(cases["United Kingdom"]));
    console.log(JSON.stringify(cases["Germany"]));
    console.log(JSON.stringify(cases["France"]));
    console.log(JSON.stringify(cases["Dutchess, New York"]));
    console.log(JSON.stringify(cases).length);
    console.log(Object.getOwnPropertyNames(cases).filter(c=>cases[c].type==="country").length + " countries");
    console.log(Object.getOwnPropertyNames(cases).filter(c=>cases[c].type==="state").length + " states");
    console.log(Object.getOwnPropertyNames(cases).filter(c=>cases[c].type==="county").length + " counties");
    console.log(Object.getOwnPropertyNames(cases).filter(c=>cases[c].population==="0").length + " zero populations");
    console.log("as of " + asof)
    await fs.writeFile("population.js", "module.exports = \n" + JSON.stringify(population, null, 2));
    const data = JSON.stringify({dates: asof, data: cases}).replace(/"([0-9]+)",/g, "$1,");
    await fs.writeFile("jhu.js", (new Date()).toString() + "\n" + data);
}
async function readFiles() {
    return {
        globCases: (await fs.readFile("./testdata/global_cases.csv", "utf8")),
        globDeaths: (await fs.readFile("./testdata/global_deaths.csv", "utf8")),
        usCases: (await fs.readFile("./testdata/us_cases.csv", "utf8")),
        usDeaths: (await fs.readFile("./testdata/us_deaths.csv", "utf8")),
        globTests: (await fs.readFile("./testdata/global_tests.csv", "utf8")),
        usTests: (await fs.readFile("./testdata/us_tests.csv", "utf8")),
        usPop: (await fs.readFile("./srcdata/co-est2019-annres.csv", "utf8")),
        globPop: (await fs.readFile("./srcdata/WPP2019_TotalPopulationBySex.csv", "utf8")),
    }
}
