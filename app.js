var express = require("express"),
    app = express(),
    csvWriter = require('csv-write-stream'),
    fs = require("fs"),
    exec = require("child_process").exec,
    request = require('request');

function writeCSVFile(body) {
    var headers = [],
        extraHeaders = ['topic', 'offset', 'partition'],
        fields = [],
        extraFields = ['topic', 'offset', 'partition'],
        createObjectHeader = function(dataObj, key, headerPrefix) {
            for (var attributename in dataObj) {
                if (typeof(dataObj[attributename]) != "object") {
                    headers.push(headerPrefix + attributename);
                    fields.push(key + attributename);
                }
            }
        };
    console.log("data length", body.length);
    createObjectHeader(body[0].value, "value.", "value_");
    headers.push("recTypeDTOs_0_recType");
    fields.push('value.recTypeDTOs[0].recType');
    createObjectHeader(body[0].value.recTypeDTOs[0].buDTOs[0], "value.recTypeDTOs[0].buDTOs[0].", "recTypeDTOs_buDTOs_");
    for (var i = 0; i <= 9; i++) {
        headers.push("purchaserInfos_" + i);
        fields.push('value.purchaserInfos[' + i + ']');
        headers.push("reasonings_" + i);
        fields.push('value.reasonings[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_buBodyLine_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].buBodyLine[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_buHeadLine_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].buHeadLine[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_buMatchCount_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].buMatchCount[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_buOfferId_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].buOfferId[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_topTaxonomies_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].topTaxonomies[' + i + ']');
        headers.push("recTypeDTOs_buDTOs_topWeights_" + i);
        fields.push('value.recTypeDTOs[0].buDTOs[0].topWeights[' + i + ']');
    }

    headers = headers.concat(extraHeaders);
    fields = fields.concat(extraFields);
    var writer = csvWriter({
        headers: headers
    });
    writer.pipe(fs.createWriteStream('sample_predicted_data.csv'));
    body.forEach(function(item, itemIndex) {
        data = [];
        fields.forEach(function(fieldName, index) {
            if (eval('item.' + fieldName)) {
                data.push(eval('item.' + fieldName));
            } else {
                data.push('');
            }
        })
        writer.write(data);
    })

    writer.end()
    console.log('CSV write complete');
}

function loadData() {
    var dataUrl = "http://semantictec.com/message/consume?topic=pc.subjectactivity.sendpcrecs&size=100000&consumerGroup=test-group&timeout=10",
        data = [];

    // exec('curl ' + dataUrl, {
    //     maxBuffer: 1024 * 10240000
    // }, function(error, stdout, stderror) {
    //     if (!error) {
    //         data = JSON.parse(stdout);
    //         data.forEach(function(url, index) {
    //             data[index].value = JSON.parse(data[index].value);
    //         })

    //         writeCSVFile(data);
    //     }
    // })
    request(dataUrl, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            data = JSON.parse(body);
            console.log("success", body.length)
            data.forEach(function(url, index) {
                data[index].value = JSON.parse(data[index].value);
            })
            writeCSVFile(data);
        } else {
            console.log("error")
        }
    })
}

loadData();
app.get('/convertData', function(req, res) {
    var dataUrl = "http://semantictec.com/message/consume?topic=pc.subjectactivity.sendpcrecs&size=1000&consumerGroup=test-group&timeout=10";

    // exec('curl ' + dataUrl, {
    //     maxBuffer: 1024 * 10240000
    // }, function(error, stdout, stderror) {
    //     if (!error) {
    //         data = JSON.parse(stdout);
    //         console.log("fetch", data.length);
    //         data.forEach(function(url, index) {
    //             data[index].value = JSON.parse(data[index].value);
    //         })

    //         res.send(data[0])
    //     }
    // })
    request(dataUrl, function(error, response, body) {
        if (!error && response.statusCode == 200) {
            body = JSON.parse(body);
            console.log("success", body.length)
            res.send({
                "length": body[0]
            }); // Send the response of the requested url to the frontend.
        } else {
            console.log("error")
        }
    })
})
app.listen(process.env.PORT || 3000);
