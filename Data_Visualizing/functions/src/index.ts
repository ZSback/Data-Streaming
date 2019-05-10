import * as functions from 'firebase-functions';

// Start writing Firebase Functions
// https://firebase.google.com/docs/functions/typescript

export const onMessageCreate = functions.database
.ref('/bot/{anything}')
.onCreate((snapshot, context) => {
    console.log(context)
})

