{%import "commonExecuteCallback.js" as commons%}

(function (pix, $) {
  var flightpredict = pix.flightpredict;

  var tdiag = $('.modal-dialog')
    .addClass('modal-dialog-flightpredictform');

  var pixiedustSpinner = $('<i class="fa fa-spinner fa-spin pixiedust-spinner"></i>')
    .prop('disabled', true)
    .css({display: 'none'});
  
  $('.pixiedust .modal-header button.close')
    .after(pixiedustSpinner);

  var restartBtn = $('.btn-restart', tdiag);
  var okBtn = $('.btn-ok', tdiag);
  var cancelBtn = $('.btn-cancel', tdiag);
  var searchBtn = $('.btn-search', tdiag);
  var predictSelectOne = $('#flight-predict-select-one', tdiag);
  var predictSelectTwo = $('#flight-predict-select-two', tdiag);
  var departureTwo = $('#flight-predict-departure-two', tdiag);
  var flightDateOne = $('#flight-predict-date-one', tdiag);
  var flightDateTwo = $('#flight-predict-date-two', tdiag);
  var messageNode = $('.flight-info .flight-predict-message', tdiag);

  var restartBtn = $('<button class="btn btn-sm btn-restart">StartOver</button>');
  var backBtn = $('<button class="btn btn-sm btn-back">Go to Notebook</button>');
  var nextBtn = $('<button class="btn btn-sm btn-next">Continue</button>');

  okBtn.before(backBtn);
  okBtn.after(nextBtn);
  okBtn.after(restartBtn);

  predictSelectOne.change(function(event){
    event.preventDefault();
    var selectedFlightOneDestAirportCode = $(this).val().split(":")[1];
    var dateTime = $(this).val().split(":")[2];
    var selectedFlightOneDepTime = flightpredict.formatDateTime(new Date(+dateTime), 'time');
    console.log('Selected Flight One dest airport ', selectedFlightOneDestAirportCode, ' at ', selectedFlightOneDepTime);

    $('.fp-path-one').text(selectedFlightOneDepTime);
    $('.fp-arr-one').text(selectedFlightOneDestAirportCode);
    departureTwo.val(selectedFlightOneDestAirportCode);

    $('.fp-arr-two').attr('style', null).html('&nbsp;');
    $('.fp-path-two').attr('style', null).html('&nbsp;');
    predictSelectTwo.empty();
  });

  predictSelectTwo.change(function(event){
    event.preventDefault();
    var selectedFlightTwoDestAirportCode = $(this).val().split(":")[1];
    var dateTime = $(this).val().split(":")[2];
    var selectedFlightTwoDepTime = flightpredict.formatDateTime(new Date(+dateTime), 'time');
    console.log('Selected Flight Two dest airport ', selectedFlightTwoDestAirportCode, ' at ', selectedFlightTwoDepTime);

    $('.fp-path-two').text(selectedFlightTwoDepTime);
    $('.fp-arr-two').text(selectedFlightTwoDestAirportCode);
  });

  searchBtn.click(function(event) {
    event.preventDefault();
    event.stopPropagation();
    messageNode.slideUp();
    searchBtn.prop('disabled', true);
    nextBtn.prop('disabled', true);
    pixiedustSpinner.css({'display': 'inline-block'});

    var suffix = $(this).attr('data-flight-suffix');
    var depAirport = "LAS";

    predictSelectTwo.empty();

    if (suffix === 'two') {
      //Get the destination airport from first flight
      depAirport = predictSelectOne.val().split(":")[1];
      $('.fp-path-two').attr('style', null).html('&nbsp;');
      $('.fp-arr-two').attr('style', null).html('&nbsp;');
    }
    else {
      predictSelectOne.empty();
      $('.fp-path-one').attr('style', null).html('&nbsp;');
      $('.fp-arr-one').attr('style', null).html('&nbsp;');
      $('.fp-path-two').attr('style', null).html('&nbsp;');
      $('.fp-arr-two').attr('style', null).html('&nbsp;');

      var currentdate = flightDateOne.datepicker('getDate');
      flightDateTwo.datepicker('option', 'minDate', currentdate);
      flightDateTwo.datepicker('setDate', currentdate);
    }
    var flightDate = $('#flight-predict-date-'+suffix, tdiag).val();
    var flightTime = $('#flight-predict-time-'+suffix, tdiag).val();

    // console.log('* FlightTime - ', flightTime)
    // var d = (new Date(flightDate));
    // d.setHours(flightTime);
    // flightTime = d.toISOString().split('T')[1];
    // console.log('* FlightTime (UTC) - ', flightTime)

    var $$pixiedust_command = "from pixiedust_flightpredict.running.runModel import *" +
      "\nprint(runFlightSearch('" + flightDate + "', '" + flightTime + "', departureAirport='" + depAirport + "'))";

    //search for flights
    {% call(results) commons.ipython_execute("$$pixiedust_command", prefix) %}
      console.log({{results["error"] or results}});
      {%if results["error"]%}
        messageNode.text(JSON.stringify({{results}}));
        messageNode.slideDown();
      {%else%}
        {% if results != '' %}
          var jsonResults = JSON.parse({{results}});
          var flights = jsonResults.flights || [];
          if (flights.length > 0) {
            flightpredict.populateFlightOptions(flights, $('#flight-predict-select-'+suffix, tdiag));

            $('.fp-arr-'+suffix).css({ opacity: 0.25, display: 'inline-block' });
            $('.fp-path-'+suffix).animate({
              opacity: 1,
              width:'35%'
            }, {
              duration: 1000,
              complete: function() {
                var ap = $('#flight-predict-select-'+suffix, tdiag).val().split(":")[1];
                $('.fp-arr-'+suffix).css({ opacity: 1 }).text(ap);
              }
            });
          } else {
            messageNode.text('No flights found for that date and time. Try a different search.');
            messageNode.slideDown();
          }
        {% endif %}
      {%endif%}
        searchBtn.prop('disabled', false);
        nextBtn.prop('disabled', false);
        pixiedustSpinner.css({'display': 'none'});
    {% endcall %}
  });

  backBtn.click(function(event) {
    event.preventDefault();
    cancelBtn.click();
  });

  restartBtn.click(function(event) {
    event.preventDefault();

    $('.tab-pane.active').removeClass('active');
    $('.tab-pane-flightpredictform').addClass('active');
    tdiag
      .removeClass('modal-dialog-flightpredictdata')
      .addClass('modal-dialog-flightpredictform');

    flightpredict.resetForm();

    searchBtn.prop('disabled', false);
    nextBtn.prop('disabled', false);
    pixiedustSpinner.css({'display': 'none'});
  });

  var doRunModel = function(flightNum, flightDateTime, depAirport, callback) {
    messageNode.slideUp();
    searchBtn.prop('disabled', true);
    nextBtn.prop('disabled', true);
    pixiedustSpinner.css({'display': 'inline-block'});
    //var dt = new Date(+flightDateTime);
    //dt = dt.toISOString();
    //dt = dt.getFullYear() + '-' + (dt.getMonth()+1) + '-' + dt.getDate();

    var $$pixiedust_command = "from pixiedust_flightpredict.running.runModel import *" +
      "\nprint(runModel('" + flightNum + "', " + flightDateTime + ", departureAirport='" + depAirport + "'))";

    //Check the flights validatity
    {% call(results) commons.ipython_execute("$$pixiedust_command", prefix) %}
      console.log({{results["error"] or results}});
      {%if results["error"]%}
        messageNode.text(JSON.stringify({{results}}));
        messageNode.slideDown();
      {%elif results != '' %}
        var jsonResults = JSON.parse({{results}});
        var flightInfo = null;

        if (jsonResults.flightInfo && jsonResults.flightInfo) {
          flightInfo = jsonResults.flightInfo;
        }

        if (callback) {
          callback(flightInfo ? jsonResults : null);
        }
      {%endif%}

      searchBtn.prop('disabled', false);
      nextBtn.prop('disabled', false);
      pixiedustSpinner.css({'display': 'none'});
    {% endcall %};
  };

  nextBtn.click(function(event) {
    event.preventDefault();

    var flightDate = flightDateOne.val();
    var flightNum = null;
    var flightTime = null;
    var errorMessage = null;

    if (!flightDate) {
      errorMessage = 'Missing flight date';
    }
    else {
      flightTime = $('#flight-predict-time-one', tdiag).val();
      if (!flightTime) {
        errorMessage = 'Missing flight time';
      }
      else {
        flightNum = predictSelectOne.val();
        if (!flightNum) {
            errorMessage = 'Missing flight number';
        }
        else {
          flightNum = flightNum.split(":")[0];
          flightDate = predictSelectOne.val().split(":")[3];
        }
      }
    }

    if (errorMessage) {
      messageNode.text(errorMessage);
      messageNode.slideDown();
    }
    else {
      var suffix = $(this).attr('data-flight-suffix');
      var depAirport = "LAS";
      
      var flightOneInfo = null;

      doRunModel(flightNum, flightDate, depAirport, function(jsonResultsOne) {
        flightOneInfo = jsonResultsOne;

        var flightNum2 = predictSelectTwo.val();
        if (flightNum2) {
          flightNum2 = flightNum2.split(":")[0];
          var flightDate2 = predictSelectTwo.val().split(":")[3];
          //Get the destination airport from first flight
          var depAirport2 = predictSelectOne.val().split(":")[1];

          doRunModel(flightNum2, flightDate2, depAirport2, function(jsonResultsTwo) {
            switchToMap(flightOneInfo, jsonResultsTwo);
          });
        }
        else {
          switchToMap(flightOneInfo);
        }
      });
    }
  });

  var switchToMap = function(flightone, flighttwo) {
    $('.tab-pane.active').removeClass('active');
    $('.tab-pane-flightpredictdata').addClass('active');

    tdiag
      .removeClass('modal-dialog-flightpredictform')
      .addClass('modal-dialog-flightpredictdata');

    if (flightone) {
      flightpredict.renderWeatherForecast(flightone, true);
      flightpredict.renderPredictionModels(flightone, true);
      flightpredict.renderFlightMap(flightone);
    }
    if (flighttwo) {
      flightpredict.renderWeatherForecast(flighttwo);
      flightpredict.renderPredictionModels(flighttwo);
      flightpredict.renderFlightMap(flighttwo);
    }
  }

  $('.chart-wrapper.flight-info .search').click(function() {
    $(this).parent().parent().toggleClass('search-flight');
  });

  $('.fp-more-link').click(function() {
    $('.flight-two').animate({
      paddingLeft: '10px',
      paddingRight: '10px',
      marginLeft: '150px',
      width:'325px'
    });
  });

  flightDateOne.datepicker({
    dateFormat: 'yy-mm-dd',
    constrainInput: true,
    minDate: new Date(),
    maxDate: '+1m',
    defaultDate: '+1d',
    gotoCurrent: true
  });

  flightDateTwo.datepicker({
    dateFormat: 'yy-mm-dd',
    constrainInput: true,
    minDate: new Date(),
    maxDate: '+1m',
    defaultDate: '+1d',
    gotoCurrent: true
  });

  flightpredict.resetForm();

  pixiedustSpinner.before('<a class="note" target="_blank" href="https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats">Fork me on GitHub</a>');
})((window.Pixiedust = window.Pixiedust || {}), jQuery)
